#!/usr/bin/env python3
"""本地回环性能基准，覆盖首次、最终次、重复作业去重与跨目录去重场景。"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import random
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

MIB = 1024 * 1024
BLOCK_SIZE = 2 * MIB
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
TRANSFER_RE = re.compile(
    r"Transferring (\d+) chunks in batches \(skipped: (\d+), relocated: (\d+), cross-file copied: (\d+)\)"
)
EXISTING_RE = re.compile(r"(\d+)/(\d+) chunks already exist")
COPY_RE = re.compile(r"Copying (\d+) chunks from other files")
LOG_MARKERS = (
    "Local dest file",
    "already exist",
    "Copying ",
    "Transferring ",
    "Updated global index",
)
SCENARIOS = (
    "first_transfer",
    "final_transfer",
    "repeat_dedup_transfer",
    "cross_dir_dedup_transfer",
)

class ManagedProcess:
    """统一处理子进程生命周期，避免异常时残留服务。"""

    def __init__(self, command: list[str], env: dict[str, str], log_path: Path) -> None:
        self.log_path = log_path
        self.log_file = log_path.open("wb")
        self.process = subprocess.Popen(
            command,
            cwd=REPO_ROOT,
            env=env,
            stdout=self.log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    def stop(self) -> None:
        if self.process.poll() is not None:
            self.log_file.close()
            return
        try:
            os.killpg(self.process.pid, signal.SIGTERM)
            self.process.wait(timeout=5)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.process.wait(timeout=5)
        finally:
            self.log_file.close()

    def assert_running(self, label: str) -> None:
        code = self.process.poll()
        if code is None:
            return
        raise RuntimeError(
            f"{label} exited unexpectedly with code {code}\n{tail_text(self.log_path)}"
        )

def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])

def run_command(command: list[str]) -> None:
    print("执行:", " ".join(command))
    subprocess.run(command, cwd=REPO_ROOT, check=True)

def request_json(url: str, method: str = "GET", payload: dict | None = None) -> dict:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} {method} {url}: {body}") from exc

def wait_until(predicate, timeout_sec: float, interval_sec: float, message: str) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(interval_sec)
    raise TimeoutError(message)

def wait_for_health(http_port: int, sync_process: ManagedProcess) -> None:
    url = f"http://127.0.0.1:{http_port}/healthz"

    def is_ready() -> bool:
        sync_process.assert_running("sync")
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                return response.status == 200
        except Exception:
            return False

    wait_until(is_ready, 20, 0.2, f"sync api did not become ready: {url}")

def wait_for_job_visible(http_port: int, job_id: str) -> None:
    url = f"http://127.0.0.1:{http_port}/api/v1/jobs/{job_id}"

    def is_visible() -> bool:
        try:
            request_json(url)
            return True
        except RuntimeError as exc:
            if "HTTP 404" in str(exc):
                return False
            raise

    wait_until(is_visible, 10, 0.2, f"job not visible in api: {job_id}")

def wait_for_terminal_job(http_port: int, job_id: str, timeout_sec: float = 120) -> dict:
    url = f"http://127.0.0.1:{http_port}/api/v1/jobs/{job_id}"
    deadline = time.time() + timeout_sec
    last_status = None
    while time.time() < deadline:
        try:
            data = request_json(url)
        except RuntimeError as exc:
            if "HTTP 404" in str(exc):
                time.sleep(0.2)
                continue
            raise
        last_status = data.get("status", "").lower()
        if last_status in {"completed", "failed", "cancelled"}:
            return data
        time.sleep(0.2)
    raise TimeoutError(f"job {job_id} did not finish, last_status={last_status}")

def sha256sum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

def build_patterns(label: str, count: int) -> list[bytes]:
    patterns: list[bytes] = []
    for index in range(count):
        seed = hashlib.sha256(f"{label}:{index}".encode("utf-8")).digest()[:8]
        patterns.append(random.Random(int.from_bytes(seed, "big")).randbytes(BLOCK_SIZE))
    return patterns

def write_pattern_file(path: Path, size_bytes: int, label: str, repeating: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    block_count = max((size_bytes + BLOCK_SIZE - 1) // BLOCK_SIZE, 1)
    patterns = build_patterns(label, 8 if repeating else block_count)
    remaining = size_bytes
    block_index = 0
    with path.open("wb") as handle:
        while remaining > 0:
            pattern = patterns[(block_index * 5 + block_index // 2) % len(patterns)] if repeating else patterns[block_index]
            chunk = pattern[: min(remaining, BLOCK_SIZE)]
            handle.write(chunk)
            remaining -= len(chunk)
            block_index += 1

def append_mixed_patterns(path: Path, append_bytes: int, base_label: str, extra_label: str) -> None:
    base_patterns = build_patterns(base_label, 8)
    new_patterns = build_patterns(extra_label, 4)
    remaining = append_bytes
    block_index = 0
    with path.open("ab") as handle:
        while remaining > 0:
            if block_index % 4 == 0:
                pattern = new_patterns[block_index % len(new_patterns)]
            else:
                pattern = base_patterns[(block_index * 3 + 1) % len(base_patterns)]
            chunk = pattern[: min(remaining, BLOCK_SIZE)]
            handle.write(chunk)
            remaining -= len(chunk)
            block_index += 1

def write_config(path: Path, http_port: int, quic_port: int, tcp_port: int, root: Path) -> None:
    agent_data = root / "agent-data"
    sync_data = root / "sync-data"
    metadata = root / "metadata"
    cert_path = agent_data / "tls" / "agent-cert.der"
    config_text = f"""sync:
  http_bind: "127.0.0.1:{http_port}"
  data_dir: {json.dumps(str(sync_data))}
  metadata: {json.dumps(str(metadata))}
  web_ui: false
  replicate_mode: tmp
  regions:
    - name: "local"
      quic_bind: "127.0.0.1:{quic_port}"
      tcp_bind: "127.0.0.1:{tcp_port}"
      quic_server_name: "127.0.0.1"
      quic_ca_cert_path: {json.dumps(str(cert_path))}

agent:
  quic_bind: "127.0.0.1:{quic_port}"
  tcp_bind: "127.0.0.1:{tcp_port}"
  data_dir: {json.dumps(str(agent_data))}
"""
    path.write_text(config_text, encoding="utf-8")

def current_log_offset(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0

def read_log_delta(path: Path, offset: int) -> tuple[int, str]:
    with path.open("rb") as handle:
        handle.seek(offset)
        data = handle.read()
        return handle.tell(), data.decode("utf-8", errors="replace")

def extract_log_summary(log_text: str) -> tuple[list[str], dict[str, int]]:
    lines = [ANSI_RE.sub("", line).strip() for line in log_text.splitlines()]
    summary = [line for line in lines if any(marker in line for marker in LOG_MARKERS)]
    metrics: dict[str, int] = {}
    for line in summary:
        if match := TRANSFER_RE.search(line):
            metrics["network_chunks"] = int(match.group(1))
            metrics["skipped_chunks"] = int(match.group(2))
            metrics["relocated_chunks"] = int(match.group(3))
            metrics["cross_file_copied"] = int(match.group(4))
        if match := EXISTING_RE.search(line):
            metrics["existing_chunks"] = int(match.group(1))
            metrics["total_chunks"] = int(match.group(2))
        if match := COPY_RE.search(line):
            metrics["copy_chunks"] = int(match.group(1))
    return summary, metrics

def ensure_hash_match(source_path: Path, dest_path: Path) -> tuple[str, str]:
    source_hash = sha256sum(source_path)
    dest_hash = sha256sum(dest_path)
    if source_hash != dest_hash:
        raise RuntimeError(
            f"hash mismatch\nsource={source_path} {source_hash}\ndest={dest_path} {dest_hash}"
        )
    return source_hash, dest_hash

def create_job(http_port: int, source_path: Path, dest_path: Path, job_type: str) -> str:
    payload = {
        "source_path": str(source_path),
        "dest_path": str(dest_path),
        "region": "local",
        "job_type": job_type,
        "priority": 5,
    }
    response = request_json(f"http://127.0.0.1:{http_port}/api/v1/jobs", "POST", payload)
    return str(response["job_id"])

def finalize_job(http_port: int, job_id: str) -> str:
    response = request_json(f"http://127.0.0.1:{http_port}/api/v1/jobs/{job_id}/final", "POST")
    return str(response["job_id"])

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

def tail_text(path: Path, lines: int = 60) -> str:
    if not path.exists():
        return f"{path} not found"
    content = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(content[-lines:])

def wait_for_agent_certificate(agent: ManagedProcess, cert_path: Path) -> None:
    def is_ready() -> bool:
        agent.assert_running("agent")
        return cert_path.exists() and cert_path.stat().st_size > 0

    wait_until(is_ready, 20, 0.2, f"agent certificate not generated: {cert_path}")

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
            "RUST_LOG": "info",
            "NO_COLOR": "1",
            "TERM": "dumb",
        }
    )

    agent_log = logs_dir / "agent.log"
    sync_log = logs_dir / "sync.log"
    agent = ManagedProcess([str(BINARY), "agent", "-c", str(config_path)], env, agent_log)
    sync = None

    try:
        cert_path = round_root / "agent-data" / "tls" / "agent-cert.der"
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
        agent.stop()

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


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
BINARY = REPO_ROOT / "target" / "release" / "hardata"


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"基准失败: {exc}", file=sys.stderr)
        raise
