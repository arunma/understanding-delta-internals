# Streaming exactly-once

Structured Streaming will rerun a micro-batch. The source offsets were reserved before the sink commit; if the driver dies after writing Parquet but before the streaming checkpoint records success, the next start replays the same batch id with the same data. Exactly-once at the Delta table means: that replay must not add the rows a second time.

Delta does this with a `txn` action in the table log, not with the streaming checkpoint alone.

## What the Delta sink writes

Each successful micro-batch is one Delta commit containing:

- `add` (and `remove`, in complete mode) for this batch's files
- `txn` with `appId = queryId` and `version = batchId`
- `commitInfo` with `operation = STREAMING UPDATE` and the query id in `operationParameters`

The fixture at version 2 is a stripped-down version of that:

```bash
uv run delta-inspect examples/fixtures/txns commit 2
```

```json
{"add":{"path":"part-00000-cccccccc-….parquet", ...}}
{"txn":{"appId":"3ae45b72-24e1-865a-a211-34987ae02f2a","version":0}}
```

Version 3 is batch 1 of the same query. After replay, the snapshot's txn map is `{queryId: 1}`.

On restart, the sink prepares batch 1 again, sees `txn[queryId] >= 1` already in the table, and skips the write. The Parquet from the incomplete attempt, if any, stays unreferenced. The streaming checkpoint can then advance.

This is why a Delta sink plus a replayable source is exactly-once *end to end* for append: the source re-delivers, the sink refuses to commit the same `(appId, version)` twice.

## Two checkpoints, two jobs

| Store | Purpose |
| --- | --- |
| Query `checkpointLocation` | Source offsets, sink commit log, state store. Answers "which batch am I on?" |
| Table `_delta_log` `txn` | Answers "has this app already committed this batch to this table?" |

Losing the query checkpoint and starting over from `startingVersion` / earliest offsets is a new query id, so the `txn` map does not suppress it. You *will* rewrite data unless you also pick a new table, overwrite, or manage `txnAppId` yourself.

Two streaming queries with the **same** `checkpointLocation` at the same time are illegal: `ConcurrentTransactionException`. Two queries with different checkpoints writing to the same table are legal; they are two `appId`s, and their appends commute like any other blind appends.

## `foreachBatch` drops the automatic `txn`

```python
stream.writeStream.foreachBatch(upsert).option("checkpointLocation", ckpt).start()
```

Inside `upsert` you are a batch writer. The Delta sink is not involved. Restart will call `upsert(batch_df, batch_id)` again with the same `batch_id`. A plain `mode("append")` duplicates the rows.

Put the `txn` back:

```python
def upsert(batch_df, batch_id):
    (
        batch_df.write.format("delta")
        .option("txnAppId", "txns-upsert")   # stable per query
        .option("txnVersion", batch_id)
        .mode("append")
        .saveAsTable("txns")
    )
```

`txnAppId` must be stable across restarts of *this* query, and unique among queries that write the same table. Using the streaming query id is fine. Using a string you chose, as above, is also fine and easier to read in the log.

`txnVersion` must be monotonically increasing per `txnAppId`. The micro-batch id already is.

If you write two tables in one `foreachBatch`, give them different `txnAppId`s (or the same id is fine too: each table has its own txn map). If you `MERGE` instead of append, the `txn` options still skip a *duplicate commit*, but only if the `MERGE` is launched through a write that carries those options. A `DeltaTable.merge(...).execute()` that does not set them will apply the merge twice on replay. Make the merge idempotent in the SQL as well (matching keys, deterministic updates), or use `DataFrameWriter` options on a library version that plumbs them into the merge commit.

Resetting the streaming checkpoint without changing `txnAppId` is the footgun already mentioned: batch ids start at 0, the table still has `txn[appId] = 4000`, and every write no-ops. Change the app id when you change the checkpoint.

## Complete mode

`outputMode("complete")` replaces the table each batch: `remove` every live file, `add` the new aggregate files, plus the same `txn`. Replay of batch `k` is still skipped if `txn[queryId] >= k`. The table's *contents* are the latest complete result; history still has every previous complete snapshot until `VACUUM`.

## What exactly-once does not cover

- Non-deterministic transformations in the query (random UDF, current timestamp used as a key). Replay produces different Parquet; the `txn` skip then hides the new bytes if the first attempt already committed, or publishes different bytes if it did not. Either way you did not replay *the same* result.
- Sinks other than Delta in the same `foreachBatch`. Kafka, JDBC, REST: you need their own idempotency.
- Source data that cannot be replayed. If the source dropped the offsets, Delta cannot invent them.

The `txn` action is a duplicate-commit filter on one table. It is a small JSON object. It is also the entire difference between "restart safe" and "restart doubles the data."
