"""Append a few rows to a local Delta table and print the log.

Requires a JVM and: uv sync --group spark

    uv run --group spark python examples/spark/01_append_and_inspect.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from session import spark_session

ROOT = Path(__file__).resolve().parent / "out" / "txns"


def main() -> None:
    spark = spark_session("delta-internals-append")
    spark.range(0, 5).selectExpr("id", "cast(id as double) * 1.5 as amount").write.format(
        "delta"
    ).mode("overwrite").save(str(ROOT))
    spark.range(5, 8).selectExpr("id", "cast(id as double) * 1.5 as amount").write.format(
        "delta"
    ).mode("append").save(str(ROOT))

    print("=== DESCRIBE HISTORY ===")
    spark.sql(f"DESCRIBE HISTORY delta.`{ROOT}`").show(truncate=False)

    log = ROOT / "_delta_log"
    print(f"=== {log} ===")
    for path in sorted(log.iterdir()):
        print(f"  {path.name}")

    latest = max(p for p in log.glob("*.json") if p.name[0].isdigit())
    print(f"\n=== {latest.name} ===")
    print(latest.read_text())
    spark.stop()


if __name__ == "__main__":
    main()
