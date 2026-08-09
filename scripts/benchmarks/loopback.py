#!/usr/bin/env python3
"""本地回环性能基准，覆盖首次、最终次、重复作业去重与跨目录去重场景。"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import sys
import time
from pathlib import Path

from scripts.testkit.harness import (
    BINARY,
    MIB,
    ManagedProcess,
    RuntimeStateGuard,
    agent_certificate_path,
    append_mixed_patterns,
    create_job,
    current_log_offset,
    ensure_hash_match,
    extract_log_summary,
    finalize_job,
    free_port,
    read_log_delta,
    run_command,
    wait_for_agent_certificate,
    wait_for_health,
    wait_for_job_visible,
    wait_for_terminal_job,
    write_config,
    write_pattern_file,
)

SCENARIOS = (
    "first_transfer",
    "final_transfer",
    "repeat_dedup_transfer",
    "cross_dir_dedup_transfer",
)

def benchmark_once_case(
    http_port: int,
    sync_log: Path,
    scenario_name: str,
    source_path: Path,
    dest_path: Path,
    transferred_bytes: int,
) -> dict:
    offset = current_log_offset(sync_log)
    start = time.perf_counter()
    job_id = create_job(http_port, source_path, dest_path, "once")
    snapshot = wait_for_terminal_job(http_port, job_id)
    elapsed = round(time.perf_counter() - start, 4)
    if snapshot["status"].lower() != "completed":
        raise RuntimeError(f"{scenario_name} did not complete: {snapshot}")
    _, log_text = read_log_delta(sync_log, offset)
    log_summary, metrics = extract_log_summary(log_text)
    source_hash, dest_hash = ensure_hash_match(source_path, dest_path)
    throughput = round((transferred_bytes / MIB) / elapsed, 2)
    result = {
        "job_id": job_id,
        "status": snapshot["status"].lower(),
        "elapsed_sec": elapsed,
        "dest_sha256": dest_hash,
        "source_sha256": source_hash,
        "log_summary": log_summary,
        **metrics,
    }
    if scenario_name == "first_transfer":
        result["effective_mib_per_sec"] = throughput
    else:
        result["effective_reuse_mib_per_sec"] = throughput
    return result

def seed_final_baseline(http_port: int, source_path: Path, dest_path: Path) -> None:
    job_id = create_job(http_port, source_path, dest_path, "once")
    snapshot = wait_for_terminal_job(http_port, job_id)
    if snapshot["status"].lower() != "completed":
        raise RuntimeError(f"final baseline did not complete: {snapshot}")
    ensure_hash_match(source_path, dest_path)

def benchmark_final_case(
    http_port: int,
    sync_log: Path,
    source_path: Path,
    dest_path: Path,
    append_bytes: int,
) -> dict:
    sync_job_id = create_job(http_port, source_path, dest_path, "sync")
    wait_for_job_visible(http_port, sync_job_id)
    append_mixed_patterns(source_path, append_bytes, "final-base", "final-append")
    offset = current_log_offset(sync_log)
    start = time.perf_counter()
    final_job_id = finalize_job(http_port, sync_job_id)
    snapshot = wait_for_terminal_job(http_port, final_job_id)
    elapsed = round(time.perf_counter() - start, 4)
    if snapshot["status"].lower() != "completed":
        raise RuntimeError(f"final transfer did not complete: {snapshot}")
    _, log_text = read_log_delta(sync_log, offset)
    log_summary, metrics = extract_log_summary(log_text)
    source_hash, dest_hash = ensure_hash_match(source_path, dest_path)
    return {
        "job_id": final_job_id,
        "status": snapshot["status"].lower(),
        "elapsed_sec": elapsed,
        "delta_mib": round(append_bytes / MIB, 2),
        "effective_delta_mib_per_sec": round((append_bytes / MIB) / elapsed, 2),
        "dest_sha256": dest_hash,
        "source_sha256": source_hash,
        "log_summary": log_summary,
        **metrics,
    }

def validate_dedup_expectations(protocol: str, results: dict) -> None:
    first_case = results["first_transfer"]
    final_case = results["final_transfer"]
    repeat_case = results["repeat_dedup_transfer"]
    cross_case = results["cross_dir_dedup_transfer"]
    if first_case.get("network_chunks", 0) <= 0:
        raise RuntimeError(f"{protocol} first transfer should send network chunks: {first_case}")
    if final_case.get("network_chunks", 0) <= 0:
        raise RuntimeError(
            f"{protocol} final transfer should keep some delta network chunks: {final_case}"
        )
    if final_case.get("cross_file_copied", 0) <= 0:
        raise RuntimeError(f"{protocol} final transfer should reuse copied chunks: {final_case}")
    if repeat_case.get("network_chunks", -1) != 0:
        raise RuntimeError(f"{protocol} repeat transfer should be zero-network: {repeat_case}")
    if repeat_case.get("cross_file_copied", 0) <= 0:
        raise RuntimeError(f"{protocol} repeat transfer should reuse local chunks: {repeat_case}")
    if cross_case.get("network_chunks", -1) != 0:
        raise RuntimeError(f"{protocol} cross-dir transfer should be zero-network: {cross_case}")
    if cross_case.get("cross_file_copied", 0) <= 0:
        raise RuntimeError(f"{protocol} cross-dir transfer should reuse cross-file chunks: {cross_case}")

def summarize_runs(runs: list[dict]) -> dict:
    if len(runs) == 1:
        return runs[0]
    merged: dict[str, object] = {"rounds": len(runs), "runs": runs}
    for key in runs[0]:
        values = [entry[key] for entry in runs]
        if isinstance(values[0], (int, float)):
            merged[key] = round(statistics.mean(values), 4 if key == "elapsed_sec" else 2)
        elif isinstance(values[0], list):
            merged[key] = values[0]
        else:
            merged[key] = values[0]
    return merged

def run_protocol_round(protocol: str, round_root: Path, file_size_bytes: int, append_bytes: int) -> dict:
    http_port = free_port()
    quic_port = free_port()
    tcp_port = free_port()
    logs_dir = round_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    config_path = round_root / "config.yaml"
    write_config(config_path, http_port, quic_port, tcp_port, round_root)

    env = os.environ.copy()
    env.update(
        {
            "HARDATA_PROTOCOL": protocol,
            "RUST_LOG": "hardata_app=debug,hardata_shared=info",
            "NO_COLOR": "1",
            "TERM": "dumb",
        }
    )

    agent_log = logs_dir / "agent.log"
    sync_log = logs_dir / "sync.log"
    runtime_state = RuntimeStateGuard()
    agent = None
    sync = None

    try:
        agent = ManagedProcess([str(BINARY), "agent", "-c", str(config_path)], env, agent_log)
        cert_path = agent_certificate_path()
        wait_for_agent_certificate(agent, cert_path)
        sync = ManagedProcess([str(BINARY), "sync", "-c", str(config_path)], env, sync_log)
        wait_for_health(http_port, sync)

        source_root = round_root / "agent-data" / "cases"
        first_source = source_root / "first" / "source.bin"
        final_source = source_root / "final" / "source.bin"
        repeat_source = source_root / "repeat" / "source.bin"
        cross_source = source_root / "cross" / "source.bin"
        first_dest = round_root / "sync-data" / "cases" / "first" / "result.bin"
        final_dest = round_root / "sync-data" / "cases" / "final" / "result.bin"
        repeat_dest = round_root / "sync-data" / "cases" / "repeat" / "result.bin"
        cross_dest = round_root / "sync-data" / "cases" / "cross" / "nested" / "result.bin"

        write_pattern_file(first_source, file_size_bytes, "first", repeating=False)
        write_pattern_file(final_source, file_size_bytes, "final-base", repeating=False)

        results = {
            "first_transfer": benchmark_once_case(
                http_port, sync_log, "first_transfer", first_source, first_dest, file_size_bytes
            )
        }

        seed_final_baseline(http_port, final_source, final_dest)
        results["final_transfer"] = benchmark_final_case(
            http_port, sync_log, final_source, final_dest, append_bytes
        )

        repeat_source.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(first_source, repeat_source)
        results["repeat_dedup_transfer"] = benchmark_once_case(
            http_port,
            sync_log,
            "repeat_dedup_transfer",
            repeat_source,
            repeat_dest,
            file_size_bytes,
        )

        cross_source.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(first_source, cross_source)
        results["cross_dir_dedup_transfer"] = benchmark_once_case(
            http_port,
            sync_log,
            "cross_dir_dedup_transfer",
            cross_source,
            cross_dest,
            file_size_bytes,
        )

        validate_dedup_expectations(protocol, results)
        return results
    finally:
        if sync is not None:
            sync.stop()
        if agent is not None:
            agent.stop()
        runtime_state.cleanup()

def aggregate_protocol_runs(protocol_runs: list[dict]) -> dict:
    return {
        scenario: summarize_runs([run[scenario] for run in protocol_runs])
        for scenario in SCENARIOS
    }

def print_summary(results: dict) -> None:
    print("\n性能摘要")
    for protocol, protocol_results in results["protocols"].items():
        print(f"{protocol.upper()}:")
        first_case = protocol_results["first_transfer"]
        final_case = protocol_results["final_transfer"]
        repeat_case = protocol_results["repeat_dedup_transfer"]
        cross_case = protocol_results["cross_dir_dedup_transfer"]
        print(
            f"  首次传输 {first_case['elapsed_sec']:.4f}s, {first_case['effective_mib_per_sec']:.2f} MiB/s, "
            f"network_chunks={int(first_case.get('network_chunks', 0))}"
        )
        print(
            f"  最终次 {final_case['elapsed_sec']:.4f}s, {final_case['effective_delta_mib_per_sec']:.2f} MiB/s, "
            f"network_chunks={int(final_case.get('network_chunks', 0))}, copied={int(final_case.get('cross_file_copied', 0))}"
        )
        print(
            f"  重复去重 {repeat_case['elapsed_sec']:.4f}s, {repeat_case['effective_reuse_mib_per_sec']:.2f} MiB/s, "
            f"network_chunks={int(repeat_case.get('network_chunks', 0))}, copied={int(repeat_case.get('cross_file_copied', 0))}"
        )
        print(
            f"  跨目录去重 {cross_case['elapsed_sec']:.4f}s, {cross_case['effective_reuse_mib_per_sec']:.2f} MiB/s, "
            f"network_chunks={int(cross_case.get('network_chunks', 0))}, copied={int(cross_case.get('cross_file_copied', 0))}"
        )

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="HarData 本地回环性能基准")
    parser.add_argument("--rounds", type=int, default=1, help="每种协议执行轮数，默认 1")
    parser.add_argument("--file-size-mib", type=int, default=96, help="基线文件大小，默认 96 MiB")
    parser.add_argument("--final-append-mib", type=int, default=16, help="final 追加大小，默认 16 MiB")
    parser.add_argument("--output", type=str, default="", help="结果 JSON 输出路径")
    parser.add_argument("--skip-build", action="store_true", help="跳过 cargo build --release")
    args = parser.parse_args()
    if args.rounds <= 0:
        parser.error("--rounds must be > 0")
    if args.file_size_mib <= 0:
        parser.error("--file-size-mib must be > 0")
    if args.final_append_mib <= 0:
        parser.error("--final-append-mib must be > 0")
    return args

def resolve_root(output_arg: str) -> tuple[Path, Path]:
    if output_arg:
        output_path = Path(output_arg).expanduser().resolve()
        root = output_path.parent
    else:
        root = Path("/tmp") / f"hardata-perf-{int(time.time())}"
        output_path = root / "results.json"
    root.mkdir(parents=True, exist_ok=True)
    return root, output_path

def main() -> int:
    args = parse_args()
    if not args.skip_build:
        run_command(["cargo", "build", "--release"])
    if not BINARY.exists():
        raise FileNotFoundError(f"binary not found: {BINARY}")

    root, output_path = resolve_root(args.output)
    file_size_bytes = args.file_size_mib * MIB
    append_bytes = args.final_append_mib * MIB

    results = {
        "root": str(root),
        "rounds": args.rounds,
        "file_size_bytes": file_size_bytes,
        "final_append_bytes": append_bytes,
        "protocols": {},
    }

    for protocol in ("tcp", "quic"):
        print(f"\n开始执行 {protocol.upper()} 基准...")
        protocol_runs = []
        for round_index in range(args.rounds):
            round_root = root / protocol / f"round-{round_index + 1}"
            if round_root.exists():
                shutil.rmtree(round_root)
            round_root.mkdir(parents=True, exist_ok=True)
            protocol_runs.append(
                run_protocol_round(protocol, round_root, file_size_bytes, append_bytes)
            )
        results["protocols"][protocol] = aggregate_protocol_runs(protocol_runs)

    output_path.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    print_summary(results)
    print(f"\n结果已写入: {output_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"基准失败: {exc}", file=sys.stderr)
        raise
