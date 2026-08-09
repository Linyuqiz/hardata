from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from scripts.testkit import harness as base

from .common import MatrixError
from .harness import Harness
from .scenarios import (
    scenario_append_mode,
    scenario_boundaries,
    scenario_cleanup_and_filters,
    scenario_dedup_and_final,
    scenario_idempotency_and_restart,
    scenario_negative_and_cancel,
    scenario_structured_tree,
)

def run_protocol(protocol: str, root: Path) -> dict[str, object]:
    harness = Harness(protocol, root / "tmp-mode")
    harness.start()
    try:
        results = {
            "boundaries": scenario_boundaries(harness),
            "structured_tree": scenario_structured_tree(harness),
            "dedup_and_final": scenario_dedup_and_final(harness),
            "cleanup_and_filters": scenario_cleanup_and_filters(harness),
            "idempotency_and_restart": scenario_idempotency_and_restart(harness),
        }
    finally:
        harness.stop()

    results["negative_and_cancel"] = scenario_negative_and_cancel(protocol, root)
    results["append_mode"] = scenario_append_mode(protocol, root)
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="HarData 全场景数据一致性矩阵测试")
    parser.add_argument("--protocol", choices=("tcp", "quic", "both"), default="both")
    parser.add_argument("--output", default="", help="结果 JSON 输出路径")
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()
    return args


def resolve_output(output_arg: str) -> tuple[Path, Path]:
    if output_arg:
        output = Path(output_arg).expanduser().resolve()
        root = output.parent
    else:
        root = Path("/tmp") / f"hardata-consistency-{int(time.time())}"
        output = root / "results.json"
    root.mkdir(parents=True, exist_ok=True)
    return root, output


def count_leaf_results(value: object) -> int:
    if isinstance(value, dict):
        own_result = int(value.get("status") in {"passed", "skipped"})
        return own_result + sum(count_leaf_results(child) for child in value.values())
    if isinstance(value, list):
        return sum(count_leaf_results(child) for child in value)
    return 0


def collect_manifest_digests(value: object, prefix: str = "") -> dict[str, str]:
    if isinstance(value, dict):
        collected: dict[str, str] = {}
        for key, child in value.items():
            path = f"{prefix}.{key}" if prefix else key
            if key == "manifest_sha256" and isinstance(child, str):
                collected[prefix] = child
            else:
                collected.update(collect_manifest_digests(child, path))
        return collected
    if isinstance(value, list):
        collected = {}
        for index, child in enumerate(value):
            collected.update(collect_manifest_digests(child, f"{prefix}[{index}]"))
        return collected
    return {}


def main() -> int:
    args = parse_args()
    if not args.skip_build:
        base.run_command(["cargo", "build", "--release"])
    if not base.BINARY.exists():
        raise FileNotFoundError(f"binary not found: {base.BINARY}")

    root, output = resolve_output(args.output)
    protocols = ("tcp", "quic") if args.protocol == "both" else (args.protocol,)
    results: dict[str, object] = {
        "root": str(root),
        "protocols": {},
        "parameters": {
            "file_boundary_sizes": [
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
            ],
            "replicate_modes": ["tmp", "append"],
            "integrity": ["manifest", "sha256", "byte_compare"],
        },
    }

    for protocol in protocols:
        print(f"\n开始执行 {protocol.upper()} 一致性矩阵...")
        results["protocols"][protocol] = run_protocol(protocol, root / protocol)
        print(f"{protocol.upper()} 一致性矩阵通过")

    if set(protocols) == {"tcp", "quic"}:
        tcp_digests = collect_manifest_digests(results["protocols"]["tcp"])
        quic_digests = collect_manifest_digests(results["protocols"]["quic"])
        if tcp_digests != quic_digests:
            raise MatrixError(
                "TCP/QUIC manifest digests differ:\n"
                f"tcp={tcp_digests}\nquic={quic_digests}"
            )
        results["cross_protocol"] = {
            "status": "passed",
            "matched_manifest_count": len(tcp_digests),
        }

    output.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    case_count = count_leaf_results(results["protocols"])
    print(f"\n一致性矩阵通过: {case_count} 个场景")
    print(f"结果已写入: {output}")
    return 0
