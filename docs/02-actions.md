# Actions

Each line of a commit JSON file is one action. Replaying the actions in version order is how you get the current table. The action set is small. Most of the complexity of Delta is "when may I emit this action" rather than "what does this action mean."

## Replay rules

Start from empty state. For each commit, for each action:

| Action | Effect on the snapshot |
| --- | --- |
| `protocol` | Replace the protocol. Last writer wins. |
| `metaData` | Replace the metadata (schema, partitions, table properties). Last writer wins. |
| `add` | The file at `path` (and deletion vector, if any) is live. |
| `remove` | That file is no longer live. The `remove` itself may be kept as a tombstone for `VACUUM`. |
| `txn` | Record `appId → version`. Used to skip duplicate writes. |
| `commitInfo` | Provenance only. Not part of the snapshot. Checkpoints drop it. |
| `cdc` | Change-data-feed file. Not a table data file. Checkpoints drop it. |

A single commit must not contain two reconciling actions for the same thing: not two `metaData`, not two `protocol`, not two `txn` with the same `appId`, not an `add` and a `remove` of the same `(path, deletion vector)`.

Look at the fixture. Version 0 establishes protocol and metadata. Version 4 removes four files and adds one. After replaying through 4, only the compacted file is live, and the streaming app's last `txn` version is still `1` because `OPTIMIZE` does not touch `txn`.

```bash
uv run delta-inspect examples/fixtures/txns commit 0
uv run delta-inspect examples/fixtures/txns snapshot
```

## `add`

```json
{
  "add": {
    "path": "part-00000-aaaaaaaa-1111-2222-3333-444444444444.c000.snappy.parquet",
    "partitionValues": {},
    "size": 841454,
    "modificationTime": 1710000000000,
    "dataChange": true,
    "stats": "{\"numRecords\":100,\"minValues\":{\"id\":1,\"amount\":1.5},\"maxValues\":{\"id\":100,\"amount\":99.0},\"nullCount\":{\"id\":0,\"amount\":0}}"
  }
}
```

`path` is relative to the table root. `partitionValues` is a string map, even when the columns are integers, because the directory names are strings. `stats` is a JSON *string* (not a nested object) in the commit file so the log stays a flat action list; checkpoints may also store `stats_parsed` as typed columns.

`dataChange` is the important boolean that most people skip. `true` means this file changes the logical contents of the table. `false` means it is a rearrangement: `OPTIMIZE`, Z-order, clustering. Streaming readers that ask for new *data* ignore `dataChange: false` adds. Two compactors that rewrite the same files still conflict physically; they just are not "new rows."

Optional fields you will see on newer tables: `deletionVector`, `baseRowId`, `defaultRowCommitVersion`, `clusteringProvider`, `tags`.

## `remove`

```json
{
  "remove": {
    "path": "part-00000-aaaaaaaa-1111-2222-3333-444444444444.c000.snappy.parquet",
    "deletionTimestamp": 1710000004000,
    "dataChange": false,
    "extendedFileMetadata": true,
    "partitionValues": {},
    "size": 841454
  }
}
```

A `remove` does not delete the bytes. It records that readers at this version and later must not see the file. The file remains on disk until `VACUUM` decides the tombstone is older than `delta.deletedFileRetentionDuration` (7 days by default) *and* no time-travel query within `delta.logRetentionDuration` still needs it.

`dataChange: false` here is the pair of the compacted `add`. A user-level `DELETE` or `MERGE` sets `dataChange: true`, because rows actually disappeared.

## `metaData`

Written when the table is created, and again whenever the schema, partition columns, or table properties change.

```json
{
  "metaData": {
    "id": "af23c9d7-fff1-4a5a-a2c8-55c59bd782aa",
    "format": {"provider": "parquet", "options": {}},
    "schemaString": "{\"type\":\"struct\",\"fields\":[...]}",
    "partitionColumns": [],
    "configuration": {"delta.checkpointInterval": "10"},
    "createdTime": 1710000000000
  }
}
```

`id` is stable for the life of the table. `schemaString` is a Spark-style JSON schema. `configuration` holds table properties: checkpoint interval, isolation level, deletion vectors, column mapping, and so on. Changing a property is itself a commit that contains a new `metaData` action.

## `protocol`

```json
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
```

or, once table features are in use:

```json
{
  "protocol": {
    "minReaderVersion": 3,
    "minWriterVersion": 7,
    "readerFeatures": ["deletionVectors"],
    "writerFeatures": ["deletionVectors", "appendOnly"]
  }
}
```

Readers that do not understand the required features must refuse the table rather than silently drop columns or skip deletion vectors. A protocol bump is a concurrent-write hazard: two writers creating a table at the same empty path, or one writer enabling a feature while another is committing, surface as `ProtocolChangedException`.

## `commitInfo`

```json
{
  "commitInfo": {
    "timestamp": 1710000001000,
    "operation": "WRITE",
    "operationParameters": {"mode": "Append", "partitionBy": "[]"},
    "readVersion": 0,
    "isolationLevel": "WriteSerializable",
    "isBlindAppend": true,
    "engineInfo": "Apache-Spark/3.5.1 Delta-Lake/3.2.0"
  }
}
```

This is what `DESCRIBE HISTORY` shows. `readVersion` is the snapshot the writer started from. `isBlindAppend` is true when the writer did not read the table: a plain `INSERT` / `append` with no subquery against the target. Blind appends are the operations that almost never conflict with each other.

`commitInfo` is not replayed into the snapshot. Checkpoints omit it. That is why log cleanup must keep the JSON commit at the cutoff checkpoint: time travel and in-commit timestamps may still need the provenance.

## `txn`

```json
{
  "txn": {
    "appId": "3ae45b72-24e1-865a-a211-34987ae02f2a",
    "version": 1,
    "lastUpdated": 1710000003000
  }
}
```

`appId` is an application identifier. `version` is a monotonically increasing number *for that app*, independent of the Delta table version. The Spark streaming sink uses the query id and the micro-batch id. `foreachBatch` does not, unless you set `txnAppId` and `txnVersion` yourself.

On commit, if the snapshot already has `txn[appId] >= this version`, Delta treats the write as a duplicate and skips it. That is the exactly-once mechanism. It is stored in the table, not in the streaming checkpoint, which is why a streaming query and a batch job can share the same idempotency key.

## Actions you will meet later

- `cdc` — a file in `_change_data/` for Change Data Feed. Readers of the table ignore it; CDF readers do not.
- `domainMetadata` — feature-specific configuration that is not table metadata (row tracking, clustering, and similar).
- `sidecar` / `checkpointMetadata` — V2 checkpoints only, never in ordinary JSON commits.

The rest of this project only needs the six actions in the title. If you can read a commit line by line and apply those rules, you can reconstruct any classic Delta table.
