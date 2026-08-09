#!/usr/bin/env python3
"""Shared assertions and fixtures for the consistency matrix."""

from __future__ import annotations

import argparse
import filecmp
import hashlib
import json
import os
import random
import stat
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from scripts.testkit import harness as base


class MatrixError(RuntimeError):
    """A consistency matrix assertion failed."""


class HttpError(MatrixError):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status


def request_json(
    url: str,
    method: str = "GET",
    payload: dict | None = None,
    headers: dict[str, str] | None = None,
) -> dict:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("Accept", "application/json")
    if data is not None:
        request.add_header("Content-Type", "application/json")
    for key, value in (headers or {}).items():
        request.add_header(key, value)

    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise HttpError(exc.code, f"HTTP {exc.code} {method} {url}: {body}") from exc


def sha256sum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def tree_manifest(root: Path) -> dict[str, dict[str, object]]:
    """Return a deterministic manifest without following symbolic links."""

    if not root.exists() and not root.is_symlink():
        raise MatrixError(f"manifest root does not exist: {root}")

    manifest: dict[str, dict[str, object]] = {}

    def visit(path: Path, relative: Path) -> None:
        metadata = path.lstat()
        key = relative.as_posix() if relative != Path() else "."
        mode = stat.S_IMODE(metadata.st_mode)

        if stat.S_ISLNK(metadata.st_mode):
            manifest[key] = {
                "type": "symlink",
                "mode": mode,
                "target": os.readlink(path),
            }
            return

        if stat.S_ISDIR(metadata.st_mode):
            manifest[key] = {"type": "dir", "mode": mode}
            children = sorted(path.iterdir(), key=lambda child: os.fsencode(child.name))
            for child in children:
                visit(child, relative / child.name)
            return

        if stat.S_ISREG(metadata.st_mode):
            manifest[key] = {
                "type": "file",
                "mode": mode,
                "size": metadata.st_size,
                "sha256": sha256sum(path),
            }
            return

        raise MatrixError(f"unsupported filesystem object in manifest: {path}")

    visit(root, Path())
    return manifest


def manifest_digest(manifest: dict[str, dict[str, object]]) -> str:
    encoded = json.dumps(manifest, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def assert_manifest_equal(source: Path, destination: Path) -> str:
    source_manifest = tree_manifest(source)
    destination_manifest = tree_manifest(destination)
    if source_manifest != destination_manifest:
        source_keys = set(source_manifest)
        destination_keys = set(destination_manifest)
        missing = sorted(source_keys - destination_keys)
        unexpected = sorted(destination_keys - source_keys)
        changed = sorted(
            key
            for key in source_keys & destination_keys
            if source_manifest[key] != destination_manifest[key]
        )
        raise MatrixError(
            "manifest mismatch\n"
            f"source={source}\n"
            f"destination={destination}\n"
            f"missing={missing[:12]}\n"
            f"unexpected={unexpected[:12]}\n"
            f"changed={changed[:12]}"
        )

    for relative, entry in source_manifest.items():
        if entry["type"] != "file":
            continue
        source_file = source / relative
        destination_file = destination / relative
        if not filecmp.cmp(source_file, destination_file, shallow=False):
            raise MatrixError(
                f"byte comparison failed: {source_file} != {destination_file}"
            )
    return manifest_digest(source_manifest)


def assert_manifest_subset(
    source_root: Path,
    destination_root: Path,
    included: list[str],
    excluded: list[str],
) -> None:
    source_manifest = tree_manifest(source_root)
    destination_manifest = tree_manifest(destination_root)
    for relative in included:
        if relative not in source_manifest:
            raise MatrixError(f"expected source entry is missing: {relative}")
        if destination_manifest.get(relative) != source_manifest[relative]:
            raise MatrixError(f"filtered entry mismatch: {relative}")
        source_entry = source_manifest[relative]
        if source_entry["type"] == "file" and not filecmp.cmp(
            source_root / relative,
            destination_root / relative,
            shallow=False,
        ):
            raise MatrixError(f"filtered byte comparison failed: {relative}")
    for relative in excluded:
        if relative in destination_manifest:
            raise MatrixError(f"excluded entry was transferred: {relative}")


def assert_no_tmp_artifacts(destination: Path, expected_manifest: dict[str, dict[str, object]]) -> None:
    if not destination.is_dir():
        sibling = Path(f"{destination}.tmp")
        if sibling.exists() or sibling.is_symlink():
            raise MatrixError(f"temporary artifact remains: {sibling}")
        return

    expected = set(expected_manifest)
    for path in destination.rglob("*"):
        relative = path.relative_to(destination).as_posix()
        if relative not in expected and path.name.endswith(".tmp"):
            raise MatrixError(f"temporary artifact remains: {path}")


def write_pattern(path: Path, size: int, label: str, repeating: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if size == 0:
        path.write_bytes(b"")
        return

    if repeating:
        block = (hashlib.sha256(label.encode("utf-8")).digest() * (1024 * 1024 // 32 + 1))[: 1024 * 1024]
        with path.open("wb") as handle:
            remaining = size
            while remaining:
                chunk = block[: min(remaining, len(block))]
                handle.write(chunk)
                remaining -= len(chunk)
        return

    random_source = random.Random(label)
    with path.open("wb") as handle:
        remaining = size
        while remaining:
            chunk = random_source.randbytes(min(1024 * 1024, remaining))
            handle.write(chunk)
            remaining -= len(chunk)


def write_binary_pattern(path: Path, size: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    block = bytes(range(256)) * 4096
    with path.open("wb") as handle:
        remaining = size
        while remaining:
            chunk = block[: min(remaining, len(block))]
            handle.write(chunk)
            remaining -= len(chunk)


def append_pattern(path: Path, size: int, label: str) -> None:
    with path.open("ab") as handle:
        block = hashlib.sha256(label.encode("utf-8")).digest() * 32768
        remaining = size
        while remaining:
            chunk = block[: min(remaining, len(block))]
            handle.write(chunk)
            remaining -= len(chunk)


def set_mode(path: Path, mode: int) -> None:
    try:
        path.chmod(mode)
    except (NotImplementedError, PermissionError):
        pass
