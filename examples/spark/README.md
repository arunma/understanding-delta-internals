# Spark examples

These write real Delta tables under `tables/txns`, `tables/stream_txns`, and `examples/spark/out/`. Spark 3.5 needs JDK 17 or 21; `session.py` switches to Homebrew `openjdk@17` when `java` on PATH is 22+.

```bash
uv sync --group spark
uv run --group spark python examples/spark/01_append_and_inspect.py
uv run --group spark python examples/spark/02_streaming_txn.py
uv run --group spark python examples/spark/03_foreach_batch.py
uv run --group spark python examples/spark/write_table.py
```

Run them from the repo root or from this directory.

The inspector works on the output the same way it works on the fixture:

```bash
uv run delta-inspect tables/stream_txns history
```
