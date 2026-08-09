from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.benchmarks import stress
from scripts.testkit import harness


class HarnessTests(unittest.TestCase):
    def test_repository_paths_resolve_from_package_location(self) -> None:
        self.assertTrue((harness.REPO_ROOT / "Cargo.toml").is_file())
        self.assertEqual(
            harness.BINARY,
            harness.REPO_ROOT / "target" / "release" / "hardata",
        )

    def test_pattern_files_are_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "first.bin"
            second = root / "second.bin"
            different = root / "different.bin"
            harness.write_pattern_file(first, harness.MIB + 17, "fixture", False)
            harness.write_pattern_file(second, harness.MIB + 17, "fixture", False)
            harness.write_pattern_file(different, harness.MIB + 17, "other", False)

            self.assertEqual(harness.sha256sum(first), harness.sha256sum(second))
            self.assertNotEqual(harness.sha256sum(first), harness.sha256sum(different))

    def test_log_summary_extracts_transfer_metrics(self) -> None:
        log_text = "\n".join(
            (
                "Transferring 7 chunks in batches (skipped: 2, relocated: 1, cross-file copied: 4)",
                "5/9 chunks already exist",
                "Copying 3 chunks from other files",
            )
        )

        summary, metrics = harness.extract_log_summary(log_text)

        self.assertEqual(len(summary), 3)
        self.assertEqual(metrics["network_chunks"], 7)
        self.assertEqual(metrics["skipped_chunks"], 2)
        self.assertEqual(metrics["relocated_chunks"], 1)
        self.assertEqual(metrics["cross_file_copied"], 4)
        self.assertEqual(metrics["existing_chunks"], 5)
        self.assertEqual(metrics["total_chunks"], 9)
        self.assertEqual(metrics["copy_chunks"], 3)

    def test_log_summary_extracts_structured_metrics(self) -> None:
        log_text = "\n".join(
            (
                'file deduplication completed operation="job.dedup_completed" job_id=job-1 reused_chunks=8 chunk_count=13',
                'global chunk index updated operation="job.global_index_updated" path=/tmp/file indexed_chunks=2',
            )
        )

        summary, metrics = harness.extract_log_summary(log_text)

        self.assertEqual(len(summary), 2)
        self.assertEqual(metrics["existing_chunks"], 8)
        self.assertEqual(metrics["total_chunks"], 13)

    def test_config_uses_isolated_runtime_directories(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "config.yaml"
            harness.write_config(config, 18080, 18443, 19000, root)
            content = config.read_text(encoding="utf-8")

            self.assertIn(str(root / "agent-data"), content)
            self.assertIn(str(root / "sync-data"), content)
            self.assertIn(str(root / "metadata"), content)
            self.assertIn(str(harness.agent_certificate_path()), content)

    def test_stress_harness_cleans_agent_when_sync_start_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            runner = stress.Harness("tcp", Path(directory))
            agent = mock.Mock()
            process_factory = mock.Mock(
                side_effect=(agent, RuntimeError("sync failed"))
            )

            with (
                mock.patch.object(stress.base, "ManagedProcess", process_factory),
                mock.patch.object(stress.base, "wait_for_agent_certificate"),
            ):
                with self.assertRaisesRegex(RuntimeError, "sync failed"):
                    runner.start()

            agent.stop.assert_called_once_with()
            self.assertIsNone(runner.agent)
            self.assertIsNone(runner.sync)

    def test_runtime_state_guard_removes_new_runtime_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            runtime = Path(directory) / ".hardata"
            guard = harness.RuntimeStateGuard(runtime)
            (runtime / "tls").mkdir(parents=True)
            (runtime / "tls" / "generated.der").write_bytes(b"test")

            guard.cleanup()

            self.assertFalse(runtime.exists())

    def test_runtime_state_guard_preserves_existing_files_and_removes_new_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            runtime = Path(directory) / ".hardata"
            existing = runtime / "tls" / "existing.der"
            existing.parent.mkdir(parents=True)
            existing.write_bytes(b"original")
            guard = harness.RuntimeStateGuard(runtime)
            existing.write_bytes(b"changed")
            (runtime / "tls" / "generated.der").write_bytes(b"test")

            guard.cleanup()

            self.assertEqual(existing.read_bytes(), b"original")
            self.assertFalse((runtime / "tls" / "generated.der").exists())


if __name__ == "__main__":
    unittest.main()
