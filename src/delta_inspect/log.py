from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

COMMIT_NAME = re.compile(r"^(\d{20})\.json$")
CHECKPOINT_NAME = re.compile(r"^(\d{20})\.checkpoint\.parquet$")
LAST_CHECKPOINT = "_last_checkpoint"


@dataclass(frozen=True)
class Commit:
    version: int
    path: Path
    actions: list[dict[str, Any]]

    def action_types(self) -> list[str]:
        types = []
        for action in self.actions:
            if not action:
                continue
            types.append(next(iter(action)))
        return types

    def commit_info(self) -> dict[str, Any] | None:
        for action in self.actions:
            if "commitInfo" in action:
                return action["commitInfo"]
        return None


@dataclass
class Snapshot:
    version: int
    protocol: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None
    files: dict[str, dict[str, Any]] = field(default_factory=dict)
    txns: dict[str, int] = field(default_factory=dict)
    from_checkpoint: int | None = None

    @property
    def table_id(self) -> str | None:
        if not self.metadata:
            return None
        return self.metadata.get("id")

    @property
    def schema(self) -> Any | None:
        if not self.metadata:
            return None
        raw = self.metadata.get("schemaString")
        if not raw:
            return None
        return json.loads(raw)


@dataclass
class TableLog:
    table_path: Path
    log_path: Path
    commits: list[Commit]
    last_checkpoint: dict[str, Any] | None
    checkpoint_version: int | None

    def commit(self, version: int) -> Commit:
        for commit in self.commits:
            if commit.version == version:
                return commit
        raise KeyError(f"no commit for version {version}")

    def snapshot(self, version: int | None = None) -> Snapshot:
        if not self.commits and self.checkpoint_version is None:
            raise ValueError(f"no Delta log found under {self.table_path}")
        target = self.commits[-1].version if version is None else version
        if version is not None and not any(c.version == version for c in self.commits):
            if self.checkpoint_version is None or version != self.checkpoint_version:
                raise KeyError(f"no commit for version {version}")

        snap = Snapshot(version=-1)
        if (
            self.checkpoint_version is not None
            and self.checkpoint_version <= target
        ):
            loaded = _try_load_checkpoint(self.log_path, self.checkpoint_version)
            if loaded is not None:
                snap = loaded

        start_after = snap.from_checkpoint if snap.from_checkpoint is not None else -1
        for commit in self.commits:
            if commit.version <= start_after:
                continue
            if commit.version > target:
                break
            _apply_commit(snap, commit)
        if snap.version != target and version is not None:
            raise KeyError(f"could not reconstruct version {version}")
        return snap


def load_table(table_path: str | Path) -> TableLog:
    root = Path(table_path).expanduser().resolve()
    log_path = root / "_delta_log"
    if not log_path.is_dir():
        raise FileNotFoundError(f"{root} is not a Delta table (missing _delta_log/)")

    commits = list(_read_commits(log_path))
    last_checkpoint = _read_last_checkpoint(log_path)
    checkpoint_version = None
    if last_checkpoint and "version" in last_checkpoint:
        checkpoint_version = int(last_checkpoint["version"])
    else:
        checkpoint_version = _latest_classic_checkpoint(log_path)

    return TableLog(
        table_path=root,
        log_path=log_path,
        commits=commits,
        last_checkpoint=last_checkpoint,
        checkpoint_version=checkpoint_version,
    )


def _read_commits(log_path: Path) -> Iterator[Commit]:
    files = []
    for path in log_path.iterdir():
        match = COMMIT_NAME.match(path.name)
        if match:
            files.append((int(match.group(1)), path))
    files.sort()
    for version, path in files:
        actions = []
        text = path.read_text(encoding="utf-8")
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            actions.append(json.loads(line))
        yield Commit(version=version, path=path, actions=actions)


def _read_last_checkpoint(log_path: Path) -> dict[str, Any] | None:
    hint = log_path / LAST_CHECKPOINT
    if not hint.is_file():
        return None
    return json.loads(hint.read_text(encoding="utf-8"))


def _latest_classic_checkpoint(log_path: Path) -> int | None:
    versions = []
    for path in log_path.iterdir():
        match = CHECKPOINT_NAME.match(path.name)
        if match:
            versions.append(int(match.group(1)))
    return max(versions) if versions else None


def _try_load_checkpoint(log_path: Path, version: int) -> Snapshot | None:
    path = log_path / f"{version:020d}.checkpoint.parquet"
    if not path.is_file():
        return None
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return None

    table = pq.read_table(path)
    snap = Snapshot(version=version, from_checkpoint=version)
    for row in table.to_pylist():
        if row.get("protocol"):
            snap.protocol = _drop_nulls(row["protocol"])
        if row.get("metaData"):
            snap.metadata = _drop_nulls(row["metaData"])
        if row.get("add") and row["add"].get("path"):
            add = _drop_nulls(row["add"])
            snap.files[add["path"]] = add
        if row.get("remove") and row["remove"].get("path"):
            snap.files.pop(row["remove"]["path"], None)
        if row.get("txn") and row["txn"].get("appId") is not None:
            snap.txns[row["txn"]["appId"]] = int(row["txn"]["version"])
    return snap


def _apply_commit(snap: Snapshot, commit: Commit) -> None:
    for action in commit.actions:
        if "protocol" in action:
            snap.protocol = action["protocol"]
        elif "metaData" in action:
            snap.metadata = action["metaData"]
        elif "add" in action:
            add = action["add"]
            snap.files[add["path"]] = add
        elif "remove" in action:
            snap.files.pop(action["remove"]["path"], None)
        elif "txn" in action:
            snap.txns[action["txn"]["appId"]] = int(action["txn"]["version"])
    snap.version = commit.version


def _drop_nulls(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _drop_nulls(v) for k, v in value.items() if v is not None}
    return value
