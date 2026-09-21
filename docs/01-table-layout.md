# Table layout

A Delta table is a directory. Data files live at the root (or under partition directories). The transaction log lives in `_delta_log/`. Everything a reader believes about the table is reconstructed from that log.

```
txns/
  part-00000-aaaaaaaa-….c000.snappy.parquet
  part-00000-bbbbbbbb-….c000.snappy.parquet
  _delta_log/
    00000000000000000000.json
    00000000000000000001.json
    00000000000000000002.json
    00000000000000000010.checkpoint.parquet
    00000000000000000010.json
    _last_checkpoint
```

The JSON files are the commits. Their names are the version number, zero-padded to 20 digits. Version `0` creates the table. Version `12` is the thirteenth commit. There are no gaps in a healthy log: if `00000000000000000011.json` is missing, version 11 was never published.

## The log is the source of truth

A Parquet file on disk is not a row in the table. It becomes part of the table only when some commit contains an `add` for its path, and it leaves the table when a later commit contains a `remove` for that path (and the same deletion vector, if the table uses them).

Consequences:

- A writer can spill Parquet for minutes before anyone else sees it. Failed jobs leave orphan files. That is expected. `VACUUM` deletes unreferenced files after a retention window; it does not consult "what is in the folder."
- You can copy a Parquet file into the table directory and it still does not exist. Spark, Trino, and every other Delta reader will ignore it.
- You can delete a Parquet file that the log still names and the table is now corrupt for any reader that needs that file. Never edit the data directory by hand.

The protocol says this directly: writers must never overwrite an existing log entry, and readers must reconstruct state only from log entries and checkpoints.

## What else is in `_delta_log/`

| File | Role |
| --- | --- |
| `{version}.json` | One commit. JSON Lines, one action per line. |
| `{version}.checkpoint.parquet` | Classic checkpoint: the snapshot at `version`, as Parquet rows that look like actions. |
| `{version}.checkpoint.{uuid}.json` / `.parquet` | V2 checkpoint. May point at sidecar files under `_sidecars/`. |
| `_last_checkpoint` | Tiny JSON hint: which checkpoint to start from. Not itself a commit. |
| `{version}.crc` | Optional version checksum for the corresponding commit. |
| `_staged_commits/{version}.{uuid}.json` | Catalog-managed tables only. A proposed commit; the catalog decides if it won. |

Hadoop also writes hidden `.filename.crc` checksums next to files. Those are filesystem bookkeeping, not Delta actions. Spark already ignores paths that start with `_` or `.` when you `read.parquet`, which is why dumping `_delta_log` as if it were data does not "see" the table.

## JSON Lines, not one JSON document

Open any commit. It is not `{ "actions": [ ... ] }`. It is one JSON object per line:

```text
{"commitInfo":{...}}
{"add":{"path":"f2",...}}
{"remove":{"path":"f1",...}}
```

That format is deliberate. A commit can add thousands of files. Readers stream the lines. Spark writes the file as a whole, atomically, so you never observe a partial commit: either `{version}.json` exists with every line, or it does not exist.

## Data files vs. log files

Data files are written by executors, in parallel, to temporary names that become the `add.path` values. They are ordinary Parquet. They carry min/max stats that later queries use for data skipping, but those stats are *copied into the `add` action*. A reader that has the log does not need to open the Parquet footer to know `numRecords` or the min `id`.

Log files are written by the driver (or by a LogStore implementation the driver calls). Throughput of data is a function of cluster size. Commit rate is a function of how fast you can create the next JSON object. That split is the whole design.

## Try it

The sample table in this repo has no Parquet files at all, only a log:

```bash
uv run delta-inspect examples/fixtures/txns files
uv run delta-inspect examples/fixtures/txns snapshot 3
```

At version 3 the snapshot lists four files. At version 4 an `OPTIMIZE` has replaced them with one. Those paths are names in JSON. Whether the bytes exist on disk is a separate question, answered later by `VACUUM`.
