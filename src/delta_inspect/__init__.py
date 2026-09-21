"""Read a Delta table's `_delta_log` without Spark.

This is a teaching reader, not a protocol-complete engine. It replays JSON
commits (and a classic Parquet checkpoint, if PyArrow is installed) so you
can see how a snapshot is built. It does not implement deletion vectors,
V2 sidecar checkpoints, or catalog-managed staged commits.
"""

from delta_inspect.log import Commit, Snapshot, load_table

__all__ = ["Commit", "Snapshot", "load_table"]
