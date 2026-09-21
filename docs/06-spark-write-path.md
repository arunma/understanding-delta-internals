# Spark write path

You never write `_delta_log` yourself. Every supported write — SQL, DataFrame, streaming sink, `MERGE`, `OPTIMIZE` — goes through `OptimisticTransaction`. The JSON you open later is that path's output, not a format you are meant to produce.

## Batch

```python
df.write.format("delta").mode("append").saveAsTable("txns")
df.write.format("delta").mode("append").save("/path/to/txns")
```

`saveAsTable` names a catalog table whose location still contains a `_delta_log`. `save` uses the path directly. Both end in the same commit.

`mode("append")` is a blind append when the plan does not read `txns`. `mode("overwrite")` prepares `remove` for every live file plus `add` for the new ones, in one commit: readers never see an empty table in between. `mode("errorIfExists")` / `ignore` are creation-time behavior, not a different commit protocol.

SQL is the same machinery:

```sql
INSERT INTO txns SELECT * FROM incoming;
MERGE INTO txns t USING incoming s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *;
OPTIMIZE txns;
```

`INSERT` without a subquery on `txns` is a blind append. `MERGE` always reads the target, so it takes the conflict profile of an update.

## Streaming

```python
(
    stream.writeStream
    .format("delta")
    .option("checkpointLocation", ckpt)
    .toTable("txns")
)
```

or `.start("/path/to/txns")`. The Delta sink, on each micro-batch, runs the same phase-1 / phase-2 commit and adds a `txn` action holding the query id and the batch id. Details on [the next page](07-streaming-exactly-once.md).

`checkpointLocation` is a Structured Streaming concern (offsets, commit log for the *query*). It is not `_delta_log`. You can put it next to the table as `txns/_checkpoints`; `VACUUM` skips directories that start with `_`.

## What to look at after a write

```sql
DESCRIBE HISTORY txns;
```

That is `commitInfo` plus the version number, one row per JSON file. Useful columns: `version`, `timestamp`, `operation`, `operationParameters`, `readVersion`, `isolationLevel`, `isBlindAppend`.

Then open the file:

```bash
uv run delta-inspect /path/to/txns history
uv run delta-inspect /path/to/txns commit 12
```

or just `cat _delta_log/00000000000000000012.json`. Read it line by line. Every claim in this project is something you can point to on one of those lines.

## The objects inside Spark, sketched

You do not need this to use Delta. You need it to map log files back to names in stack traces.

| Name | Job |
| --- | --- |
| `DeltaLog` | Cached per table path. Knows how to list the log, load snapshots, checkpoint. |
| `Snapshot` | Immutable file list + metadata at one version. |
| `OptimisticTransaction` | A write in progress: the snapshot it started from, the actions it will commit. |
| `ConflictChecker` | Compares this transaction to each winning commit in the retry window. |
| `LogStore` | `read`, `listFrom`, `write(..., overwrite=false)`. Storage-specific atomicity. |
| `Checkpoints` | `postCommit` hook that writes `{N}.checkpoint.parquet` and `_last_checkpoint`. |

A batch append roughly does: `DeltaLog.forTable` → `startTransaction` → write Parquet via a `FileFormatWriter` → `txn.commit(actions, operation)`. Streaming's `DeltaSink.addBatch` does that plus the `txn` action.

## Idempotent batch writes

The same `txn` mechanism is available outside streaming:

```python
(
    df.write.format("delta")
    .option("txnAppId", "nightly-load")
    .option("txnVersion", 20260921)
    .mode("append")
    .saveAsTable("txns")
)
```

Rerunning the job with the same pair is a no-op once it has committed. Bump `txnVersion` when the payload is actually new. If you reset a streaming checkpoint and keep the old `txnAppId`, batch 0 of the new query looks like a duplicate of batch 0 of the old one and is dropped — so change `txnAppId` when you change checkpoint.

## User metadata

```python
df.write.format("delta").option("userMetadata", "release-2026-09-21").mode("append").saveAsTable("txns")
```

That string lands in `commitInfo` and shows up in `DESCRIBE HISTORY`. It does not affect conflict checking. Use it when you want the log to explain *why* a commit exists, not just that it does.
