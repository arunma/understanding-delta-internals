# Optimistic concurrency

Delta does not lock the table for the duration of a write. It copies the snapshot, stages files, then tries to install its JSON at the next version. If someone else got there first, it *rechecks* rather than immediately failing. The public docs call this three stages: read, write, validate-and-commit. The validation is the interesting part.

## The retry loop

```text
attempt = snapshot.version + 1
loop:
    try write _delta_log/{attempt}.json   # create if absent
    if success: return
    winning = read commits [original.version+1, attempt]
    if logical conflict with winning:
        fail (Concurrent*Exception)
    else:
        attempt += 1          # same actions, next version
```

Spark logs this as:

```text
Attempting to commit version 6 with 13 actions with Serializable isolation level
No logical conflicts with deltas [6, 7), retrying.
Attempting to commit version 7 with 13 actions with Serializable isolation level
```

The Parquet files from phase 1 are reused. Retry is metadata-only: new JSON, same `add` paths. That is why two appends that race are cheap to resolve. A `MERGE` that raced with another `MERGE` on the same files cannot be retried this way; the read set is stale, and Spark aborts so the caller can rerun the whole job.

## What "conflict" means

Physical occupancy of a version number is not a logical conflict. Two writers can both try to create `006.json`; one wins the filename, the other asks: "given that 6 happened, is my action list still legal as 7?"

Delta answers that by comparing read and write sets, under the table's isolation level.

| Pair | Can they both commit? |
| --- | --- |
| INSERT + INSERT | No conflict (blind appends). Loser retries as the next version. |
| INSERT + UPDATE/DELETE/MERGE | Can conflict, if the mutating job read the partition the insert landed in. |
| UPDATE/DELETE/MERGE + UPDATE/DELETE/MERGE | Can conflict, if they touch the same files. |
| Compaction (`dataChange=false`) + INSERT | No conflict. Compaction + compaction, or compaction + a mutate of the same files, can conflict. |

"Same files" is the real predicate. Partitioning by the columns you filter on makes concurrent `MERGE`s of different dates touch different `add`/`remove` sets, so they commute. A `MERGE` whose condition does not mention the partition column may scan the whole table and then conflict with everyone.

The exceptions you actually see:

| Exception | Meaning |
| --- | --- |
| `ConcurrentAppendException` | Someone added files in a partition this transaction read. |
| `ConcurrentDeleteReadException` | Someone removed a file this transaction read. |
| `ConcurrentDeleteDeleteException` | Someone already removed a file this transaction also removes (two compactors, usually). |
| `MetadataChangedException` | Schema or table properties changed under you. |
| `ProtocolChangedException` | Protocol / table features changed, or two writers created the table at once. |
| `ConcurrentTransactionException` | Two streaming queries with the same checkpoint / `appId` committing at once. |

## Isolation levels

Table property `delta.isolationLevel`, default `WriteSerializable`.

**Serializable.** There must exist a serial order of the committed writes that matches both the history *and* what each write read. A long-running `DELETE` that started at v0 cannot commit at v2 if an `INSERT` already created v1 with rows the delete would have seen. The delete must abort (and typically retry from the new snapshot).

**WriteSerializable (default).** Only the *writes* need a serial order. Reads of the table during the transaction are allowed to have missed concurrent inserts. The same delete-and-insert race is permitted: history shows insert at v1, delete at v2, but the resulting data is as if the delete ran first and the insert's rows survive. A reader who time-travels through history can therefore observe a state that, logically, "never existed" as a serial execution of the SQL that was submitted.

Reads themselves always use snapshot isolation: a query that opened v4 keeps seeing v4 until it asks again, regardless of the write isolation level.

Blind appends (`isBlindAppend: true` in `commitInfo`) do not read the target table. Under either isolation level they do not conflict with each other, which is why "two appends usually don't conflict" is the common case, not a special case.

## Why this is safe for the log

The loser never mutates the winner's JSON. Create-if-absent gives mutual exclusion on a version. Conflict checking gives serializability (or write-serializability) of the *actions*. Together they are OCC with a log that is also the commit record.

What OCC does not give you:

- It does not make phase 1 atomic. Partial Parquet from a killed job stays until `VACUUM`.
- It does not lock rows. Two `MERGE`s on the same primary keys in the same files will collide on those files, not on the keys.
- It does not coordinate streaming checkpoints. Two queries with one `checkpointLocation` are a `ConcurrentTransactionException`, not a silent merge of offsets.

For the `txns` table in this repo, versions 1–3 are all appends (one batch, two streaming). If they had raced, each would have retried one version later with the same `add` paths and the same `txn` payload. Version 4 is an `OPTIMIZE`: it removes those files. An append that *read* the table would conflict with that optimize; a blind append would not, and would land beside the compacted file as version 5.
