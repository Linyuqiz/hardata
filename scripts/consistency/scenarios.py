from __future__ import annotations

import os
import stat
from pathlib import Path

from scripts.testkit import harness as base

from .common import (
    HttpError,
    MatrixError,
    append_pattern,
    assert_manifest_equal,
    assert_manifest_subset,
    assert_no_tmp_artifacts,
    request_json,
    set_mode,
    tree_manifest,
    write_binary_pattern,
    write_pattern,
)
from .harness import Harness

def scenario_boundaries(harness: Harness) -> dict[str, object]:
    source = harness.agent_data / "matrix" / "boundaries"
    source.mkdir(parents=True, exist_ok=True)
    set_mode(source, 0o755)
    sizes = [
        0,
        1,
        255,
        256,
        1023,
        1024,
        262143,
        262144,
        262145,
        2097151,
        2097152,
        2097153,
        8388607,
        8388608,
        8388609,
    ]
    for index, size in enumerate(sizes):
        path = source / f"size-{size:08d}.bin"
        if index % 3 == 0:
            write_pattern(path, size, f"boundary-{size}")
        elif index % 3 == 1:
            write_pattern(path, size, f"boundary-repeat-{size}", repeating=True)
        else:
            write_binary_pattern(path, size)
        set_mode(path, 0o640 if index % 2 else 0o644)

    sparse = source / "sparse-16m.bin"
    with sparse.open("wb") as handle:
        handle.truncate(16 * base.MIB + 1)
    set_mode(sparse, 0o644)

    destination = harness.sync_data / "matrix" / "boundaries"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(b"destination has the wrong type")
    boundaries = harness.run_and_compare("boundaries", source, destination)

    empty_source = harness.agent_data / "matrix" / "empty-overwrite.bin"
    empty_source.write_bytes(b"")
    empty_destination = harness.sync_data / "matrix" / "empty-overwrite.bin"
    empty_destination.write_bytes(b"non-empty destination")
    empty_overwrite = harness.run_and_compare(
        "empty_file_overwrite", empty_source, empty_destination
    )
    return {"directory_boundaries": boundaries, "empty_file_overwrite": empty_overwrite}


def scenario_structured_tree(harness: Harness) -> dict[str, object]:
    source = harness.agent_data / "matrix" / "structured"
    (source / "empty-dir").mkdir(parents=True, exist_ok=True)
    (source / "nested" / "empty-deep").mkdir(parents=True, exist_ok=True)
    (source / "links").mkdir(parents=True, exist_ok=True)
    set_mode(source, 0o750)
    set_mode(source / "empty-dir", 0o700)
    set_mode(source / "nested", 0o750)
    write_pattern(source / "plain.txt", 8193, "structured-plain")
    write_binary_pattern(source / "nested" / "binary data.bin", 65537)
    set_mode(source / "nested" / "binary data.bin", 0o444)
    write_pattern(source / ".hidden", 17, "structured-hidden")
    write_pattern(source / "中文 空格" / "文件 [1].dat", 131071, "structured-unicode")
    long_name = "long-" + ("x" * 220) + ".bin"
    write_pattern(source / "中文 空格" / long_name, 777, "structured-long")
    set_mode(source / "plain.txt", 0o640)
    set_mode(source / "中文 空格" / "文件 [1].dat", 0o600)

    symlink_count = 0
    if os.name == "posix":
        os.symlink("../plain.txt", source / "links" / "plain-link")
        os.symlink("../nested", source / "links" / "nested-link")
        symlink_count = 2

    destination = harness.sync_data / "matrix" / "structured"
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "links").mkdir(parents=True, exist_ok=True)
    (destination / "links" / "plain-link").write_bytes(b"wrong object type")
    result = harness.run_and_compare("structured_tree", source, destination)
    result["symlinks"] = symlink_count

    empty_source = harness.agent_data / "matrix" / "empty-root"
    empty_source.mkdir(parents=True, exist_ok=True)
    empty_destination = harness.sync_data / "matrix" / "empty-root"
    empty_destination.write_bytes(b"wrong destination type")
    result["empty_root"] = harness.run_and_compare(
        "empty_root_directory", empty_source, empty_destination
    )

    if os.name == "posix":
        root_file_link = harness.agent_data / "matrix" / "root-file-link"
        root_dir_link = harness.agent_data / "matrix" / "root-dir-link"
        os.symlink("structured/plain.txt", root_file_link)
        os.symlink("structured/nested", root_dir_link)
        root_file_destination = harness.sync_data / "matrix" / "root-file-link"
        root_dir_destination = harness.sync_data / "matrix" / "root-dir-link"
        root_file_destination.write_bytes(b"wrong root symlink")
        root_dir_destination.mkdir(parents=True, exist_ok=True)
        result["root_symlinks"] = {
            "file": harness.run_and_compare(
                "root_file_symlink", root_file_link, root_file_destination
            ),
            "directory": harness.run_and_compare(
                "root_directory_symlink", root_dir_link, root_dir_destination
            ),
        }
    return result


def scenario_dedup_and_final(harness: Harness) -> dict[str, object]:
    source = harness.agent_data / "matrix" / "dedup"
    source.mkdir(parents=True, exist_ok=True)
    write_pattern(source / "base.bin", 4 * base.MIB, "dedup-base", repeating=True)
    source.joinpath("duplicate.bin").write_bytes((source / "base.bin").read_bytes())
    write_pattern(source / "mixed.bin", 6 * base.MIB, "dedup-mixed")
    destination = harness.sync_data / "matrix" / "dedup"
    destination.mkdir(parents=True, exist_ok=True)
    write_pattern(destination / "stale.bin", 1024, "stale")

    first = harness.run_and_compare("dedup_first", source, destination)
    (destination / "base.bin").write_bytes(b"destination drift")
    set_mode(destination / "base.bin", 0o600)
    repaired = harness.run_and_compare("dedup_full_repair", source, destination, "full")

    final_source = harness.agent_data / "matrix" / "final.bin"
    final_destination = harness.sync_data / "matrix" / "final.bin"
    write_pattern(final_source, 3 * base.MIB, "final-base", repeating=True)
    sync_job = harness.submit(final_source, final_destination, "sync")
    base.wait_for_job_visible(harness.http_port, sync_job)
    append_pattern(final_source, 1 * base.MIB, "final-append")
    final_job = base.finalize_job(harness.http_port, sync_job)
    snapshot = harness.wait(final_job)
    if snapshot.get("status", "").lower() != "completed":
        raise MatrixError(f"final transfer did not complete: {snapshot}")
    final_digest = assert_manifest_equal(final_source, final_destination)
    return {
        "first": first,
        "full_repair": repaired,
        "final_append": {
            "status": "passed",
            "job_id": final_job,
            "manifest_sha256": final_digest,
        },
    }


def scenario_cleanup_and_filters(harness: Harness) -> dict[str, object]:
    source = harness.agent_data / "matrix" / "cleanup"
    source.mkdir(parents=True, exist_ok=True)
    write_pattern(source / "keep.bin", 4097, "cleanup-keep")
    write_pattern(source / "remove.bin", 8193, "cleanup-remove")
    write_pattern(source / "nested" / "keep.bin", 123, "cleanup-nested")
    destination = harness.sync_data / "matrix" / "cleanup"
    destination.mkdir(parents=True, exist_ok=True)
    write_pattern(destination / "stale.bin", 100, "cleanup-stale")
    write_pattern(destination / "nested" / "stale.bin", 100, "cleanup-stale-nested")
    first = harness.run_and_compare("cleanup_initial", source, destination)

    (source / "remove.bin").unlink()
    deleted_source = harness.run_and_compare("cleanup_deleted_source", source, destination)

    filtered_source = harness.agent_data / "matrix" / "filtered"
    filtered_source.mkdir(parents=True, exist_ok=True)
    write_pattern(filtered_source / "keep.txt", 513, "filter-keep")
    write_pattern(filtered_source / "drop.log", 513, "filter-drop")
    write_pattern(filtered_source / "nested" / "keep.dat", 513, "filter-nested-keep")
    write_pattern(filtered_source / "nested" / "drop.dat", 513, "filter-nested-drop")
    filtered_destination = harness.sync_data / "matrix" / "filtered"
    filtered_destination.mkdir(parents=True, exist_ok=True)
    write_pattern(filtered_destination / "old.txt", 31, "filter-old")
    filtered_job = harness.submit(
        filtered_source,
        filtered_destination,
        exclude_regex=[r"drop"],
    )
    filtered_snapshot = harness.wait(filtered_job)
    if filtered_snapshot.get("status", "").lower() != "completed":
        raise MatrixError(f"filtered job did not complete: {filtered_snapshot}")
    assert_manifest_subset(
        filtered_source,
        filtered_destination,
        ["keep.txt", "nested", "nested/keep.dat"],
        ["drop.log", "nested/drop.dat"],
    )
    if not (filtered_destination / "old.txt").exists():
        raise MatrixError("filtered sync unexpectedly removed an unrelated destination file")

    included_destination = harness.sync_data / "matrix" / "included"
    included_job = harness.submit(
        filtered_source,
        "matrix/included",
        include_regex=[r"keep"],
    )
    included_snapshot = harness.wait(included_job)
    if included_snapshot.get("status", "").lower() != "completed":
        raise MatrixError(f"included job did not complete: {included_snapshot}")
    assert_manifest_subset(
        filtered_source,
        included_destination,
        ["keep.txt", "nested/keep.dat"],
        ["drop.log", "nested/drop.dat"],
    )
    return {
        "initial": first,
        "deleted_source": deleted_source,
        "filtered": {"status": "passed", "job_id": filtered_job},
        "included": {"status": "passed", "job_id": included_job},
    }


def scenario_idempotency_and_restart(harness: Harness) -> dict[str, object]:
    source = harness.agent_data / "matrix" / "idempotent.bin"
    write_pattern(source, 2 * base.MIB + 17, "idempotency")
    destination = harness.sync_data / "matrix" / "idempotent.bin"
    key = f"consistency-{harness.protocol}-idempotency"
    headers = {"Idempotency-Key": key}
    first_job = harness.submit(source, destination, headers=headers)
    second_job = harness.submit(source, destination, headers=headers)
    if first_job != second_job:
        raise MatrixError(f"idempotency returned different jobs: {first_job} != {second_job}")
    snapshot = harness.wait(first_job)
    if snapshot.get("status", "").lower() != "completed":
        raise MatrixError(f"idempotent job did not complete: {snapshot}")
    idempotent_digest = assert_manifest_equal(source, destination)
    conflict_destination = harness.sync_data / "matrix" / "idempotent-conflict.bin"
    try:
        harness.submit(source, conflict_destination, headers=headers)
    except HttpError as exc:
        if exc.status != 409:
            raise
        conflict_result = {"status": "passed", "http_status": exc.status}
    else:
        raise MatrixError("idempotency key was accepted for a different payload")

    restart_source = harness.agent_data / "matrix" / "restart.bin"
    write_pattern(restart_source, 8 * base.MIB + 3, "restart")
    restart_destination = harness.sync_data / "matrix" / "restart.bin"
    restart_job = harness.submit(restart_source, restart_destination)
    harness.restart_sync()
    restart_snapshot = harness.wait(restart_job)
    if restart_snapshot.get("status", "").lower() != "completed":
        raise MatrixError(f"job did not recover after sync restart: {restart_snapshot}")
    restart_digest = assert_manifest_equal(restart_source, restart_destination)
    return {
        "idempotency": {
            "status": "passed",
            "job_id": first_job,
            "manifest_sha256": idempotent_digest,
            "conflict": conflict_result,
        },
        "restart_recovery": {
            "status": "passed",
            "job_id": restart_job,
            "manifest_sha256": restart_digest,
        },
    }


def scenario_negative_and_cancel(protocol: str, root: Path) -> dict[str, object]:
    cancel_root = root / "cancel"
    harness = Harness(protocol, cancel_root, stability_threshold_secs=20)
    harness.start()
    try:
        source = harness.agent_data / "matrix" / "cancel.bin"
        write_pattern(source, 4 * base.MIB, "cancel")
        destination = harness.sync_data / "matrix" / "cancel.bin"
        sync_job = harness.submit(source, destination, "sync")
        base.wait_for_job_visible(harness.http_port, sync_job)
        request_json(f"http://127.0.0.1:{harness.http_port}/api/v1/jobs/{sync_job}", "DELETE")
        snapshot = harness.wait(sync_job)
        if snapshot.get("status", "").lower() != "cancelled":
            raise MatrixError(f"cancelled job has unexpected status: {snapshot}")
        if destination.exists() or Path(f"{destination}.tmp").exists():
            raise MatrixError("cancelled job published a destination")

        missing_source = harness.agent_data / "matrix" / "missing.bin"
        missing_destination = harness.sync_data / "matrix" / "missing.bin"
        missing_job = harness.submit(missing_source, missing_destination)
        missing_snapshot = harness.wait(missing_job)
        if missing_snapshot.get("status", "").lower() not in {"completed", "failed"}:
            raise MatrixError(f"missing-source job has unexpected status: {missing_snapshot}")
        if missing_destination.exists() and tree_manifest(missing_destination) != {".": {"type": "dir", "mode": stat.S_IMODE(missing_destination.stat().st_mode)}}:
            raise MatrixError("missing-source job created unexpected destination content")

        outside = root / "outside"
        outside.mkdir(parents=True, exist_ok=True)
        escape = harness.sync_data / "escape"
        if os.name == "posix":
            escape.unlink(missing_ok=True)
            os.symlink(outside, escape)
            escape_source = harness.agent_data / "matrix" / "escape.bin"
            write_pattern(escape_source, 1024, "escape")
            try:
                escape_job = harness.submit(escape_source, escape / "output.bin")
            except HttpError as exc:
                if exc.status != 400:
                    raise
                escape_result = {"status": "passed", "rejected_at": "api"}
            else:
                escape_snapshot = harness.wait(escape_job)
                if escape_snapshot.get("status", "").lower() != "failed":
                    raise MatrixError(f"symlink escape job was not rejected: {escape_snapshot}")
                escape_result = {"status": "passed", "rejected_at": "scheduler"}
            if (outside / "output.bin").exists():
                raise MatrixError("destination symlink escape wrote outside sync.data_dir")
        else:
            escape_result = {"status": "skipped", "reason": "requires POSIX symlink support"}

        source_for_negative = harness.agent_data / "matrix" / "negative.bin"
        write_pattern(source_for_negative, 1, "negative")
        try:
            harness.submit(source_for_negative, "../outside.bin")
        except HttpError as exc:
            if exc.status != 400:
                raise
            traversal_result = {"status": "passed", "http_status": exc.status}
        else:
            raise MatrixError("path traversal destination was accepted")

        try:
            harness.submit(source_for_negative, harness.sync_data / "invalid-regex", include_regex=["["])
        except HttpError as exc:
            if exc.status != 400:
                raise
            regex_result = {"status": "passed", "http_status": exc.status}
        else:
            raise MatrixError("invalid include regex was accepted")

        return {
            "cancel": {"status": "passed", "job_id": sync_job},
            "missing_source": {"status": "passed", "job_id": missing_job},
            "symlink_escape": escape_result,
            "path_traversal": traversal_result,
            "invalid_regex": regex_result,
        }
    finally:
        harness.stop()


def scenario_append_mode(protocol: str, root: Path) -> dict[str, object]:
    harness = Harness(protocol, root / "append-mode", replicate_mode="append")
    harness.start()
    try:
        source = harness.agent_data / "matrix" / "append-mode.bin"
        write_pattern(source, 3 * base.MIB + 7, "append-mode", repeating=True)
        destination = harness.sync_data / "matrix" / "append-mode.bin"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"old destination")
        return harness.run_and_compare("append_mode", source, destination)
    finally:
        harness.stop()
