#!/usr/bin/env python3
"""Print a compact operational report from dbt's target/run_results.json."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def load_report(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise FileNotFoundError(f"dbt run results not found: {path}")

    payload = json.loads(path.read_text(encoding="utf-8"))
    results = payload.get("results")
    if not isinstance(results, list):
        raise ValueError("Invalid dbt run_results.json: expected a 'results' array.")
    return results


def render_report(results: list[dict[str, Any]]) -> str:
    lines = [
        "ATLAS DBT EXECUTION REPORT",
        "=" * 76,
        f"{'MODEL':42} {'STATUS':12} {'SECONDS':>9} {'ROWS AFFECTED':>13}",
        "-" * 76,
    ]

    total_rows = 0
    total_time = 0.0

    for item in results:
        unique_id = str(item.get("unique_id", "unknown"))
        model = unique_id.rsplit(".", 1)[-1][:42]
        status = str(item.get("status", "unknown"))
        elapsed = float(item.get("execution_time") or 0.0)
        adapter = item.get("adapter_response") or {}
        rows = adapter.get("rows_affected")
        if rows is None:
            rows = adapter.get("rowcount")
        row_text = str(rows) if rows is not None else "n/a"

        if isinstance(rows, (int, float)):
            total_rows += int(rows)
        total_time += elapsed

        lines.append(f"{model:42} {status:12} {elapsed:9.2f} {row_text:>13}")

    lines.extend([
        "-" * 76,
        f"Models reported: {len(results)}",
        f"Total adapter rows affected (where reported): {total_rows}",
        f"Total execution time: {total_time:.2f}s",
    ])
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--results",
        type=Path,
        default=Path("atlas_analytics/target/run_results.json"),
        help="Path to dbt run_results.json",
    )
    args = parser.parse_args(argv)

    try:
        results = load_report(args.results)
        print(render_report(results))
        failures = [
            item for item in results
            if item.get("status") in {"error", "fail"}
        ]
        return 1 if failures else 0
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(f"Unable to build dbt report: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
