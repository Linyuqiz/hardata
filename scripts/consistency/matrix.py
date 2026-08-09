"""Run the cross-protocol data consistency matrix."""

import sys

from .runner import main


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"一致性矩阵失败: {exc}", file=sys.stderr)
        raise
