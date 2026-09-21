"""Write a real Delta table under tables/txns for log introspection.

Creates the table, appends until a checkpoint is written (default interval 10),
then OPTIMIZE so the log has add, remove, commitInfo, and a checkpoint.

    uv run --group spark python examples/spark/write_table.py
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from session import spark_session

REPO = Path(__file__).resolve().parents[2]
TABLE = REPO / "tables" / "txns"
COMMITS_AFTER_CREATE = 11


def main() -> None:
    if TABLE.exists():
        shutil.rmtree(TABLE)
    TABLE.parent.mkdir(parents=True, exist_ok=True)

    spark = spark_session("delta-internals-write")
    (
        spark.range(0, 10, 1, 1)
        .selectExpr("id", "cast(id as double) * 1.5 as amount")
        .write.format("delta")
        .mode("overwrite")
        .save(str(TABLE))
    )
    for i in range(1, COMMITS_AFTER_CREATE + 1):
        start = i * 10
        (
            spark.range(start, start + 3, 1, 1)
            .selectExpr("id", "cast(id as double) * 1.5 as amount")
            .write.format("delta")
            .mode("append")
            .save(str(TABLE))
        )
    spark.sql(f"OPTIMIZE delta.`{TABLE}`").show()

    print("=== DESCRIBE HISTORY ===")
    spark.sql(f"DESCRIBE HISTORY delta.`{TABLE}`").select(
        "version", "timestamp", "operation", "operationParameters"
    ).show(truncate=False)

    log = TABLE / "_delta_log"
    print(f"=== {log} ===")
    for path in sorted(log.iterdir()):
        print(f"  {path.name}")
    spark.stop()


if __name__ == "__main__":
    main()
