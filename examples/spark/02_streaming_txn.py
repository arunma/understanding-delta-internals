"""Write a short stream into Delta and show the txn action Spark added.

    uv run --group spark python examples/spark/02_streaming_txn.py
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent))
from session import spark_session

OUT = Path(__file__).resolve().parent / "out"
TABLE = OUT / "stream_txns"
CKPT = OUT / "stream_ckpt"


def main() -> None:
    spark = spark_session("delta-internals-stream")
    spark.range(0, 6).selectExpr("id", "id % 3 as rate").write.mode("overwrite").parquet(
        str(OUT / "rate_src")
    )

    stream = (
        spark.readStream.format("parquet")
        .schema("id LONG, rate LONG")
        .option("maxFilesPerTrigger", 1)
        .load(str(OUT / "rate_src"))
        .withColumn("amount", F.col("id").cast("double"))
        .drop("rate")
    )
    query = (
        stream.writeStream.format("delta")
        .option("checkpointLocation", str(CKPT))
        .outputMode("append")
        .start(str(TABLE))
    )
    query.processAllAvailable()
    query.stop()

    print("=== DESCRIBE HISTORY ===")
    spark.sql(f"DESCRIBE HISTORY delta.`{TABLE}`").select(
        "version", "operation", "operationParameters"
    ).show(truncate=False)

    log = TABLE / "_delta_log"
    for path in sorted(log.glob("*.json")):
        text = path.read_text()
        if '"txn"' in text:
            print(f"=== {path.name} (has txn) ===")
            print(text)
            break
    spark.stop()


if __name__ == "__main__":
    main()
