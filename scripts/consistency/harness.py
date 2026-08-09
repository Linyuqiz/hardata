from __future__ import annotations

import json
import os
import time
from pathlib import Path

from scripts.testkit import harness as base

from .common import (
    MatrixError,
    assert_manifest_equal,
    assert_no_tmp_artifacts,
    request_json,
    tree_manifest,
)

class Harness:
    def __init__(
        self,
        protocol: str,
        root: Path,
        replicate_mode: str = "tmp",
        stability_threshold_secs: int = 0,
    ) -> None:
        self.protocol = protocol
        self.root = root
        self.agent_data = root / "agent-data"
        self.sync_data = root / "sync-data"
        self.metadata = root / "metadata"
        self.agent_data.mkdir(parents=True, exist_ok=True)
        self.sync_data.mkdir(parents=True, exist_ok=True)
        self.metadata.mkdir(parents=True, exist_ok=True)
        self.http_port = base.free_port()
        self.quic_port = base.free_port()
        self.tcp_port = base.free_port()
        self.replicate_mode = replicate_mode
        self.config_path = root / "config.yaml"
        self.agent_log = root / "agent.log"
        self.sync_log = root / "sync.log"
        self.agent: base.ManagedProcess | None = None
        self.sync: base.ManagedProcess | None = None
        self.runtime_state = base.RuntimeStateGuard()
        self.stability_threshold_secs = stability_threshold_secs
        self.config_path.write_text(
            f"""sync:
  http_bind: "127.0.0.1:{self.http_port}"
  data_dir: {json.dumps(str(self.sync_data))}
  metadata: {json.dumps(str(self.metadata))}
  web_ui: false
  allow_external_destinations: false
  stability_threshold_secs: {stability_threshold_secs}
  replicate_mode: {replicate_mode}
  regions:
    - name: "local"
      quic_bind: "127.0.0.1:{self.quic_port}"
      tcp_bind: "127.0.0.1:{self.tcp_port}"

agent:
  quic_bind: "127.0.0.1:{self.quic_port}"
  tcp_bind: "127.0.0.1:{self.tcp_port}"
  data_dir: {json.dumps(str(self.agent_data))}
""",
            encoding="utf-8",
        )

    def _env(self) -> dict[str, str]:
        env = os.environ.copy()
        env.update(
            {
                "HARDATA_PROTOCOL": self.protocol,
                "RUST_LOG": "hardata_app=debug,hardata_shared=info",
                "NO_COLOR": "1",
                "TERM": "dumb",
            }
        )
        return env

    def start(self) -> None:
        if self.agent is not None or self.sync is not None:
            raise MatrixError("harness is already running")
        env = self._env()
        self.agent = base.ManagedProcess(
            [str(base.BINARY), "agent", "-c", str(self.config_path)],
            env,
            self.agent_log,
        )
        try:
            base.wait_for_agent_certificate(self.agent, base.agent_certificate_path())
            self.sync = base.ManagedProcess(
                [str(base.BINARY), "sync", "-c", str(self.config_path)],
                env,
                self.sync_log,
            )
            base.wait_for_health(self.http_port, self.sync)
        except Exception:
            self.stop()
            raise

    def restart_sync(self) -> None:
        if self.sync is None:
            raise MatrixError("sync is not running")
        self.sync.stop()
        self.sync = base.ManagedProcess(
            [str(base.BINARY), "sync", "-c", str(self.config_path)],
            self._env(),
            self.sync_log,
        )
        base.wait_for_health(self.http_port, self.sync)

    def stop(self) -> None:
        try:
            if self.sync is not None:
                self.sync.stop()
                self.sync = None
            if self.agent is not None:
                self.agent.stop()
                self.agent = None
        finally:
            self.runtime_state.cleanup()

    def destination_path(self, destination: Path | str) -> Path:
        path = Path(destination)
        return path if path.is_absolute() else self.sync_data / path

    def submit(
        self,
        source: Path,
        destination: Path | str,
        job_type: str = "once",
        include_regex: list[str] | None = None,
        exclude_regex: list[str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> str:
        payload = {
            "source_path": str(source),
            "dest_path": str(destination),
            "region": "local",
            "job_type": job_type,
            "priority": 5,
            "include_regex": include_regex or [],
            "exclude_regex": exclude_regex or [],
        }
        response = request_json(
            f"http://127.0.0.1:{self.http_port}/api/v1/jobs",
            "POST",
            payload,
            headers,
        )
        return str(response["job_id"])

    def wait(self, job_id: str, timeout_sec: float = 180) -> dict:
        return base.wait_for_terminal_job(self.http_port, job_id, timeout_sec)

    def run_and_compare(
        self,
        name: str,
        source: Path,
        destination: Path | str,
        job_type: str = "once",
        include_regex: list[str] | None = None,
        exclude_regex: list[str] | None = None,
    ) -> dict[str, object]:
        destination_path = self.destination_path(destination)
        started = time.perf_counter()
        job_id = self.submit(
            source,
            destination,
            job_type,
            include_regex,
            exclude_regex,
        )
        snapshot = self.wait(job_id)
        if snapshot.get("status", "").lower() != "completed":
            raise MatrixError(f"{name} did not complete: {snapshot}")
        digest = assert_manifest_equal(source, destination_path)
        assert_no_tmp_artifacts(destination_path, tree_manifest(source))
        return {
            "status": "passed",
            "job_id": job_id,
            "elapsed_sec": round(time.perf_counter() - started, 4),
            "manifest_sha256": digest,
        }
