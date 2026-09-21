# Understanding Delta Internals

A Delta table is Parquet plus `_delta_log`. The log is the source of truth.

This repo walks that log: commit protocol, snapshot reconstruction, Spark writers, and streaming exactly-once. Start at [docs/00-overview.md](docs/00-overview.md).

```bash
uv sync
uv run delta-inspect tables/txns
uv run delta-inspect tables/stream_txns
uv run pytest
```

`tables/txns` is batch (appends, checkpoint at v10, OPTIMIZE at v12). `tables/stream_txns` is a streaming sink (`STREAMING UPDATE` + `txn`). No Spark needed to read either. To regenerate: `uv sync --group spark`, then `write_table.py` / `02_streaming_txn.py`.

- [Table layout](docs/01-table-layout.md)
- [Actions](docs/02-actions.md)
- [Commit protocol](docs/03-commit-protocol.md)
- [Optimistic concurrency](docs/04-optimistic-concurrency.md)
- [Checkpoints and readers](docs/05-checkpoints-and-readers.md)
- [Spark write path](docs/06-spark-write-path.md)
- [Streaming exactly-once](docs/07-streaming-exactly-once.md)
- [Pipelining](docs/08-pipelining.md)
