"""foreachBatch is not idempotent unless you set txnAppId / txnVersion.

    uv run --group spark python examples/spark/03_foreach_batch.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from session import spark_session

OUT = Path(__file__).resolve().parent / "out"
TABLE = OUT / "foreach_txns"
CKPT = OUT / "foreach_ckpt"
SRC = OUT / "foreach_src"


def upsert(batch_df, batch_id: int) -> None:
    (
        batch_df.write.format("delta")
        .option("txnAppId", "txns-upsert")
        .option("txnVersion", batch_id)
        .mode("append")
        .save(str(TABLE))
    )


def main() -> None:
    spark = spark_session("delta-internals-foreach")
    spark.range(0, 4).write.mode("overwrite").parquet(str(SRC))

    query = (
        spark.readStream.format("parquet")
        .schema("id LONG")
        .load(str(SRC))
        .writeStream.foreachBatch(upsert)
        .option("checkpointLocation", str(CKPT))
        .start()
    )
    query.processAllAvailable()
    query.stop()

    # Re-running the same batch_id with the same txnAppId must not duplicate rows.
    upsert(spark.read.parquet(str(SRC)), 0)

    count = spark.read.format("delta").load(str(TABLE)).count()
    print(f"row count after replay of batch 0: {count}")
    spark.sql(f"DESCRIBE HISTORY delta.`{TABLE}`").select(
        "version", "operation"
    ).show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
