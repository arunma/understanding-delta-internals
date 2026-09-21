# Understanding Delta Internals

A Delta table is Parquet plus `_delta_log`. The log is the source of truth.

This repo walks that log: commit protocol, snapshot reconstruction, Spark writers, and streaming exactly-once. Start at [docs/00-overview.md](docs/00-overview.md).

```bash
uv sync
uv run delta-inspect examples/fixtures/txns
uv run pytest
```

Spark examples (JDK 17): `uv sync --group spark`, then `uv run --group spark python examples/spark/01_append_and_inspect.py`.

- [Table layout](docs/01-table-layout.md)
- [Actions](docs/02-actions.md)
- [Commit protocol](docs/03-commit-protocol.md)
- [Optimistic concurrency](docs/04-optimistic-concurrency.md)
- [Checkpoints and readers](docs/05-checkpoints-and-readers.md)
- [Spark write path](docs/06-spark-write-path.md)
- [Streaming exactly-once](docs/07-streaming-exactly-once.md)
- [Pipelining](docs/08-pipelining.md)
