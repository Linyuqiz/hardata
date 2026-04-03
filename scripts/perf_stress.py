#!/usr/bin/env python3
"""生产侧补充压测：小文件批量与多任务并发。"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import shutil
import statistics
import sys
import time
from pathlib import Path

import perf_loopback as base


def file_bytes(label: str, index: int, size: int) -> bytes:
    seed = hashlib.sha256(f"{label}:{index}".encode("utf-8")).digest()[:8]
    return random.Random(int.from_bytes(seed, "big")).randbytes(size)


def write_small_tree(root: Path, file_count: int, file_size_kib: int, label: str) -> int:
    root.mkdir(parents=True, exist_ok=True)
    size = file_size_kib * 1024
    total_bytes = 0
    for index in range(file_count):
        rel = Path(f"group_{index // 128:03d}") / f"bucket_{(index // 16) % 8:02d}" / f"file_{index:05d}.bin"
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        data = file_bytes(label, index, size)
        path.write_bytes(data)
        total_bytes += len(data)
    return total_bytes


def write_large_files(root: Path, job_count: int, file_size_mib: int, label: str) -> int:
    root.mkdir(parents=True, exist_ok=True)
    total_bytes = 0
    size = file_size_mib * base.MIB
    for index in range(job_count):
        path = root / f"job_{index:02d}.bin"
        base.write_pattern_file(path, size, f"{label}-{index}", repeating=False)
        total_bytes += size
    return total_bytes


def tree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        rel = path.relative_to(root).as_posix().encode("utf-8")
        digest.update(rel)
        digest.update(b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def sum_log_metrics(log_text: str) -> dict[str, int]:
    metrics = {
        "network_chunks": 0,
        "skipped_chunks": 0,
        "relocated_chunks": 0,
        "cross_file_copied": 0,
        "copy_chunks": 0,
        "existing_chunks": 0,
        "total_chunks": 0,
        "indexed_files": 0,
    }
    for raw in log_text.splitlines():
        line = base.ANSI_RE.sub("", raw).strip()
        if match := base.TRANSFER_RE.search(line):
            metrics["network_chunks"] += int(match.group(1))
            metrics["skipped_chunks"] += int(match.group(2))
            metrics["relocated_chunks"] += int(match.group(3))
            metrics["cross_file_copied"] += int(match.group(4))
        if match := base.COPY_RE.search(line):
            metrics["copy_chunks"] += int(match.group(1))
        if match := base.EXISTING_RE.search(line):
            metrics["existing_chunks"] += int(match.group(1))
            metrics["total_chunks"] += int(match.group(2))
        if "Updated global index for" in line:
            metrics["indexed_files"] += 1
    return metrics


class Harness:
    def __init__(self, protocol: str, round_root: Path) -> None:
        self.protocol = protocol
        self.round_root = round_root
        self.http_port = base.free_port()
        self.quic_port = base.free_port()
        self.tcp_port = base.free_port()
        self.logs_dir = round_root / "logs"
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.config_path = round_root / "config.yaml"
        base.write_config(
            self.config_path, self.http_port, self.quic_port, self.tcp_port, round_root
        )
        env = dict(**base.os.environ)
        env.update(
            {
                "HARDATA_PROTOCOL": protocol,
                "RUST_LOG": "info",
                "NO_COLOR": "1",
                "TERM": "dumb",
            }
        )
        self.env = env
        self.agent = None
        self.sync = None
        self.sync_log = self.logs_dir / "sync.log"

    def start(self) -> None:
        self.agent = base.ManagedProcess(
            [str(base.BINARY), "agent", "-c", str(self.config_path)],
            self.env,
            self.logs_dir / "agent.log",
        )
        cert_path = self.round_root / "agent-data" / "tls" / "agent-cert.der"
        base.wait_for_agent_certificate(self.agent, cert_path)
        self.sync = base.ManagedProcess(
            [str(base.BINARY), "sync", "-c", str(self.config_path)],
            self.env,
            self.sync_log,
        )
        base.wait_for_health(self.http_port, self.sync)

    def stop(self) -> None:
        if self.sync is not None:
            self.sync.stop()
        if self.agent is not None:
            self.agent.stop()


def benchmark_tree_once(
    harness: Harness, source_root: Path, dest_root: Path, total_bytes: int
) -> dict:
    offset = base.current_log_offset(harness.sync_log)
    start = time.perf_counter()
    job_id = base.create_job(harness.http_port, source_root, dest_root, "once")
    snapshot = base.wait_for_terminal_job(harness.http_port, job_id, timeout_sec=300)
    elapsed = round(time.perf_counter() - start, 4)
    if snapshot["status"].lower() != "completed":
        raise RuntimeError(f"tree once failed: {snapshot}")
    _, log_text = base.read_log_delta(harness.sync_log, offset)
    metrics = sum_log_metrics(log_text)
    source_hash = tree_digest(source_root)
    dest_hash = tree_digest(dest_root)
    if source_hash != dest_hash:
        raise RuntimeError(f"tree digest mismatch: {source_root} {dest_root}")
    return {
        "job_id": job_id,
        "status": snapshot["status"].lower(),
        "elapsed_sec": elapsed,
        "files_per_sec": round(metrics["indexed_files"] / elapsed, 2),
        "effective_mib_per_sec": round((total_bytes / base.MIB) / elapsed, 2),
        "source_tree_sha256": source_hash,
        "dest_tree_sha256": dest_hash,
        **metrics,
    }


def benchmark_concurrent_jobs(
    harness: Harness, source_root: Path, dest_root: Path, job_count: int, total_bytes: int
) -> dict:
    offset = base.current_log_offset(harness.sync_log)
    start = time.perf_counter()
    job_ids = [
        base.create_job(
            harness.http_port,
            source_root / f"job_{index:02d}.bin",
            dest_root / f"job_{index:02d}.bin",
            "once",
        )
        for index in range(job_count)
    ]
    snapshots = [
        base.wait_for_terminal_job(harness.http_port, job_id, timeout_sec=300)
        for job_id in job_ids
    ]
    elapsed = round(time.perf_counter() - start, 4)
    failed = [snapshot for snapshot in snapshots if snapshot["status"].lower() != "completed"]
    if failed:
        raise RuntimeError(f"concurrent jobs failed: {failed}")
    _, log_text = base.read_log_delta(harness.sync_log, offset)
    metrics = sum_log_metrics(log_text)
    for index in range(job_count):
        source_hash, dest_hash = base.ensure_hash_match(
            source_root / f"job_{index:02d}.bin",
            dest_root / f"job_{index:02d}.bin",
        )
        if source_hash != dest_hash:
            raise RuntimeError(f"concurrent hash mismatch for job {index}")
    return {
        "job_count": job_count,
        "elapsed_sec": elapsed,
        "jobs_per_sec": round(job_count / elapsed, 2),
        "effective_mib_per_sec": round((total_bytes / base.MIB) / elapsed, 2),
        **metrics,
    }


def validate_results(protocol: str, result: dict) -> None:
    small_first = result["small_files_first_transfer"]
    small_repeat = result["small_files_repeat_dedup"]
    concurrent = result["concurrent_jobs_transfer"]
    if small_first["network_chunks"] <= 0:
        raise RuntimeError(f"{protocol} small-files first transfer must send network chunks")
    if small_repeat["network_chunks"] != 0:
        raise RuntimeError(f"{protocol} small-files repeat transfer should be zero-network")
    if small_repeat["cross_file_copied"] <= 0:
        raise RuntimeError(f"{protocol} small-files repeat transfer should reuse local chunks")
    if concurrent["network_chunks"] <= 0:
        raise RuntimeError(f"{protocol} concurrent jobs should send network chunks")


def run_round(
    protocol: str,
    round_root: Path,
    small_file_count: int,
    small_file_kib: int,
    concurrent_jobs: int,
    concurrent_file_mib: int,
) -> dict:
    harness = Harness(protocol, round_root)
    harness.start()
    try:
        source_root = round_root / "agent-data" / "stress"
        sync_root = round_root / "sync-data" / "stress"
        small_first_source = source_root / "small-first"
        small_repeat_source = source_root / "small-repeat"
        small_first_dest = sync_root / "small-first"
        small_repeat_dest = sync_root / "small-repeat"
        concurrent_source = source_root / "concurrent"
        concurrent_dest = sync_root / "concurrent"

        small_total_bytes = write_small_tree(
            small_first_source, small_file_count, small_file_kib, f"small-{protocol}"
        )
        result = {
            "small_files_first_transfer": benchmark_tree_once(
                harness, small_first_source, small_first_dest, small_total_bytes
            )
        }

        shutil.copytree(small_first_source, small_repeat_source)
        result["small_files_repeat_dedup"] = benchmark_tree_once(
            harness, small_repeat_source, small_repeat_dest, small_total_bytes
        )

        concurrent_total_bytes = write_large_files(
            concurrent_source,
            concurrent_jobs,
            concurrent_file_mib,
            f"concurrent-{protocol}",
        )
        result["concurrent_jobs_transfer"] = benchmark_concurrent_jobs(
            harness,
            concurrent_source,
            concurrent_dest,
            concurrent_jobs,
            concurrent_total_bytes,
        )
        validate_results(protocol, result)
        return result
    finally:
        harness.stop()


def summarize_runs(runs: list[dict]) -> dict:
    if len(runs) == 1:
        return runs[0]
    merged: dict[str, object] = {"rounds": len(runs), "runs": runs}
    for key in runs[0]:
        values = [entry[key] for entry in runs]
        if isinstance(values[0], (int, float)):
            merged[key] = round(statistics.mean(values), 4 if key == "elapsed_sec" else 2)
        else:
            merged[key] = values[0]
    return merged


def aggregate_protocol_runs(runs: list[dict]) -> dict:
    return {name: summarize_runs([entry[name] for entry in runs]) for name in runs[0]}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="HarData 生产侧补充压测")
    parser.add_argument("--rounds", type=int, default=1)
    parser.add_argument("--small-file-count", type=int, default=512)
    parser.add_argument("--small-file-kib", type=int, default=64)
    parser.add_argument("--concurrent-jobs", type=int, default=4)
    parser.add_argument("--concurrent-file-mib", type=int, default=24)
    parser.add_argument("--output", type=str, default="")
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()
    for name in (
        "rounds",
        "small_file_count",
        "small_file_kib",
        "concurrent_jobs",
        "concurrent_file_mib",
    ):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be > 0")
    return args


def resolve_output(output_arg: str) -> tuple[Path, Path]:
    if output_arg:
        output_path = Path(output_arg).expanduser().resolve()
        root = output_path.parent
    else:
        root = Path("/tmp") / f"hardata-stress-{int(time.time())}"
        output_path = root / "results.json"
    root.mkdir(parents=True, exist_ok=True)
    return root, output_path


def print_summary(results: dict) -> None:
    print("\n压测摘要")
    for protocol, data in results["protocols"].items():
        small_first = data["small_files_first_transfer"]
        small_repeat = data["small_files_repeat_dedup"]
        concurrent = data["concurrent_jobs_transfer"]
        print(f"{protocol.upper()}:")
        print(
            f"  小文件首次 {small_first['elapsed_sec']:.4f}s, {small_first['files_per_sec']:.2f} files/s, network_chunks={int(small_first['network_chunks'])}"
        )
        print(
            f"  小文件去重 {small_repeat['elapsed_sec']:.4f}s, {small_repeat['files_per_sec']:.2f} files/s, network_chunks={int(small_repeat['network_chunks'])}, copied={int(small_repeat['cross_file_copied'])}"
        )
        print(
            f"  并发任务 {concurrent['elapsed_sec']:.4f}s, {concurrent['jobs_per_sec']:.2f} jobs/s, {concurrent['effective_mib_per_sec']:.2f} MiB/s"
        )


def main() -> int:
    args = parse_args()
    if not args.skip_build:
        base.run_command(["cargo", "build", "--release"])
    if not base.BINARY.exists():
        raise FileNotFoundError(f"binary not found: {base.BINARY}")
    root, output_path = resolve_output(args.output)
    results = {
        "root": str(root),
        "rounds": args.rounds,
        "small_file_count": args.small_file_count,
        "small_file_kib": args.small_file_kib,
        "concurrent_jobs": args.concurrent_jobs,
        "concurrent_file_mib": args.concurrent_file_mib,
        "protocols": {},
    }
    for protocol in ("tcp", "quic"):
        print(f"\n开始执行 {protocol.upper()} 补充压测...")
        runs = []
        for index in range(args.rounds):
            round_root = root / protocol / f"round-{index + 1}"
            if round_root.exists():
                shutil.rmtree(round_root)
            round_root.mkdir(parents=True, exist_ok=True)
            runs.append(
                run_round(
                    protocol,
                    round_root,
                    args.small_file_count,
                    args.small_file_kib,
                    args.concurrent_jobs,
                    args.concurrent_file_mib,
                )
            )
        results["protocols"][protocol] = aggregate_protocol_runs(runs)
    output_path.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    print_summary(results)
    print(f"\n结果已写入: {output_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"压测失败: {exc}", file=sys.stderr)
        raise
