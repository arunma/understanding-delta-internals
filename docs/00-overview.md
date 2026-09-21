# Delta internals

This project is a close reading of how a Delta table actually commits. The log is the table. Parquet files are payload. A file that no commit currently names is not part of the table, even if it sits in the folder.

The walkthrough follows one path end to end:

1. What lives on disk
2. What a JSON commit contains
3. How a writer publishes the next version
4. How concurrent writers retry or abort
5. How a reader reconstructs the live file list
6. How Spark (batch and streaming) uses that same path
7. Why overlapping "data staging" is safe only if commits stay ordered

## How to read this

Start with this page, then go in order:

| Doc | What it answers |
| --- | --- |
| [01 · Table layout](01-table-layout.md) | What a Delta folder is, and what is *not* the table |
| [02 · Actions](02-actions.md) | `add`, `remove`, `metaData`, `protocol`, `commitInfo`, `txn` |
| [03 · Commit protocol](03-commit-protocol.md) | Two-phase write and atomic "create if absent" |
| [04 · Optimistic concurrency](04-optimistic-concurrency.md) | Conflict checks, isolation, retries |
| [05 · Checkpoints and readers](05-checkpoints-and-readers.md) | `_last_checkpoint`, snapshot reconstruction |
| [06 · Spark write path](06-spark-write-path.md) | Batch and streaming APIs, `DESCRIBE HISTORY` |
| [07 · Streaming exactly-once](07-streaming-exactly-once.md) | The `txn` action, `foreachBatch` |
| [08 · Pipelining](08-pipelining.md) | Why phase 1 can overlap and phase 2 cannot |

You do not need Spark to follow the argument. Open the sample table and read the log:

```bash
uv run delta-inspect examples/fixtures/txns
uv run delta-inspect examples/fixtures/txns history
uv run delta-inspect examples/fixtures/txns commit 2
uv run delta-inspect examples/fixtures/txns snapshot 3
```

The fixture is a hand-written `_delta_log` for a table named `txns`: create, append, two streaming batches, then an `OPTIMIZE`. The Parquet files named in those commits are not present; they do not need to be. The log is enough to reconstruct the file list.

## The one-sentence model

A write has two phases. Executors write Parquet (invisible). The driver creates the next numbered JSON file in `_delta_log/` with an atomic create-if-absent. Once that file exists, the commit is done. Readers load the newest checkpoint, apply the JSON commits after it, and that is the table.

## Sources

The behavior described here is the filesystem commit protocol used by Delta on object stores and HDFS, as specified in the [Delta protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md) and implemented by the Spark connector. Storage-specific atomicity and the Spark APIs are from:

- Michael Armbrust et al., [*Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores*](https://www.vldb.org/pvldb/vol13/p3411-armbrust.pdf), PVLDB 13(12), 2020
- [Concurrency control](https://docs.delta.io/latest/concurrency-control.html)
- [Isolation levels](https://docs.databricks.com/aws/en/optimizations/isolation/isolation-levels)
- [Streaming reads and writes](https://docs.delta.io/latest/delta-streaming.html)
- [Storage / LogStore](https://docs.delta.io/latest/delta-storage.html)

Catalog-managed tables (Unity Catalog style, with `_staged_commits/`) are a later variant where the catalog, not the filesystem, ratifies the winner. They are noted where they diverge. The rest of this project is the classic log.
