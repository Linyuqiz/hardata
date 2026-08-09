"""Shared process, HTTP, fixture, and log helpers for end-to-end scripts."""

from __future__ import annotations

import hashlib
import json
import os
import random
import re
import signal
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
BINARY = REPO_ROOT / "target" / "release" / "hardata"
AGENT_CERTIFICATE = REPO_ROOT / ".hardata" / "tls" / "agent-cert-127.0.0.1.der"

MIB = 1024 * 1024
BLOCK_SIZE = 2 * MIB
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
TRANSFER_RE = re.compile(
    r"Transferring (\d+) chunks in batches \(skipped: (\d+), relocated: (\d+), cross-file copied: (\d+)\)"
)
EXISTING_RE = re.compile(r"(\d+)/(\d+) chunks already exist")
COPY_RE = re.compile(r"Copying (\d+) chunks from other files")
STRUCTURED_DEDUP_RE = re.compile(
    r'operation="job\.dedup_completed".*?reused_chunks=(\d+).*?chunk_count=(\d+)'
)
LOG_MARKERS = (
    "Local dest file",
    "already exist",
    "Copying ",
    "Transferring ",
    "Updated global index",
    'operation="job.dedup_completed"',
    'operation="job.global_index_updated"',
)


class RuntimeStateGuard:
    """Restore or remove the repository runtime directory after a test run."""

    def __init__(self, root: Path = REPO_ROOT / ".hardata") -> None:
        self.root = root
        self._root_existed = root.exists()
        self._files: dict[Path, bytes] = {}
        self._directories: set[Path] = set()
        if not self._root_existed:
            return

        for path in root.rglob("*"):
            relative = path.relative_to(root)
            if path.is_dir():
                self._directories.add(relative)
            elif path.is_file():
                self._files[relative] = path.read_bytes()

    def cleanup(self) -> None:
        if not self._root_existed:
            if self.root.exists():
                shutil.rmtree(self.root)
            return

        self.root.mkdir(parents=True, exist_ok=True)
        for relative in sorted(self._directories, key=lambda value: len(value.parts)):
            (self.root / relative).mkdir(parents=True, exist_ok=True)

        for relative, contents in self._files.items():
            path = self.root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            if not path.exists() or path.read_bytes() != contents:
                path.write_bytes(contents)

        for path in sorted(self.root.rglob("*"), key=lambda value: len(value.parts), reverse=True):
            relative = path.relative_to(self.root)
            if path.is_file() and relative not in self._files:
                path.unlink()
            elif path.is_dir() and relative not in self._directories:
                try:
                    path.rmdir()
                except OSError:
                    pass

    def __enter__(self) -> "RuntimeStateGuard":
        return self

    def __exit__(self, *_: object) -> None:
        self.cleanup()


class ManagedProcess:
    """Manage a child process and persist combined output."""

    def __init__(self, command: list[str], env: dict[str, str], log_path: Path) -> None:
        self.log_path = log_path
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_file = log_path.open("wb")
        try:
            self.process = subprocess.Popen(
                command,
                cwd=REPO_ROOT,
                env=env,
                stdout=self.log_file,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except Exception:
            self.log_file.close()
            raise

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


def agent_certificate_path() -> Path:
    """Return the certificate location used by the production agent."""

    return AGENT_CERTIFICATE


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


def wait_until(
    predicate: Callable[[], bool],
    timeout_sec: float,
    interval_sec: float,
    message: str,
) -> None:
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


def wait_for_agent_certificate(agent: ManagedProcess, cert_path: Path) -> None:
    def is_ready() -> bool:
        agent.assert_running("agent")
        return cert_path.exists() and cert_path.stat().st_size > 0

    wait_until(is_ready, 20, 0.2, f"agent certificate not generated: {cert_path}")


def sha256sum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ensure_hash_match(source_path: Path, dest_path: Path) -> tuple[str, str]:
    source_hash = sha256sum(source_path)
    dest_hash = sha256sum(dest_path)
    if source_hash != dest_hash:
        raise RuntimeError(
            f"hash mismatch\nsource={source_path} {source_hash}\ndest={dest_path} {dest_hash}"
        )
    return source_hash, dest_hash


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
            pattern = (
                patterns[(block_index * 5 + block_index // 2) % len(patterns)]
                if repeating
                else patterns[block_index]
            )
            chunk = pattern[: min(remaining, BLOCK_SIZE)]
            handle.write(chunk)
            remaining -= len(chunk)
            block_index += 1


def append_mixed_patterns(
    path: Path,
    append_bytes: int,
    base_label: str,
    extra_label: str,
) -> None:
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


def write_config(
    path: Path,
    http_port: int,
    quic_port: int,
    tcp_port: int,
    root: Path,
) -> None:
    agent_data = root / "agent-data"
    sync_data = root / "sync-data"
    metadata = root / "metadata"
    cert_path = agent_certificate_path()
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
        if match := STRUCTURED_DEDUP_RE.search(line):
            metrics["existing_chunks"] = int(match.group(1))
            metrics["total_chunks"] = int(match.group(2))
    return summary, metrics


def create_job(http_port: int, source_path: Path, dest_path: Path, job_type: str) -> str:
    payload = {
        "source_path": str(source_path),
        "dest_path": str(dest_path),
        "region": "local",
        "job_type": job_type,
        "priority": 5,
    }
    response = request_json(
        f"http://127.0.0.1:{http_port}/api/v1/jobs",
        "POST",
        payload,
    )
    return str(response["job_id"])


def finalize_job(http_port: int, job_id: str) -> str:
    response = request_json(
        f"http://127.0.0.1:{http_port}/api/v1/jobs/{job_id}/final",
        "POST",
    )
    return str(response["job_id"])


def tail_text(path: Path, lines: int = 60) -> str:
    if not path.exists():
        return f"{path} not found"
    content = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(content[-lines:])
