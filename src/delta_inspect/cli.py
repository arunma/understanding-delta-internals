from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from delta_inspect.log import TableLog, load_table


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Inspect a Delta table's _delta_log without Spark."
    )
    parser.add_argument("table", help="Path to the Delta table root")
    parser.add_argument(
        "command",
        nargs="?",
        default="summary",
        choices=["summary", "history", "commit", "snapshot", "files"],
        help="What to print (default: summary)",
    )
    parser.add_argument(
        "version",
        nargs="?",
        type=int,
        help="Commit version for `commit` or `snapshot`",
    )
    args = parser.parse_args(argv)

    try:
        log = load_table(args.table)
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.command == "summary":
        _print_summary(log)
    elif args.command == "history":
        _print_history(log)
    elif args.command == "commit":
        if args.version is None:
            print("error: commit requires a version", file=sys.stderr)
            return 2
        _print_commit(log, args.version)
    elif args.command == "snapshot":
        _print_snapshot(log, args.version)
    elif args.command == "files":
        _print_files(log, args.version)
    return 0


def _print_summary(log: TableLog) -> None:
    latest = log.commits[-1].version if log.commits else None
    snap = log.snapshot()
    print(f"table:      {log.table_path}")
    print(f"log:        {log.log_path}")
    print(f"commits:    {len(log.commits)}  (v0 .. v{latest})")
    print(f"checkpoint: {log.checkpoint_version if log.checkpoint_version is not None else 'none'}")
    print(f"protocol:   {json.dumps(snap.protocol)}")
    print(f"table id:   {snap.table_id}")
    print(f"live files: {len(snap.files)}")
    print(f"txn apps:   {len(snap.txns)}")
    if snap.txns:
        for app_id, version in sorted(snap.txns.items()):
            print(f"  {app_id}: {version}")


def _print_history(log: TableLog) -> None:
    print(f"{'version':>8}  {'operation':<18} {'actions'}")
    for commit in log.commits:
        info = commit.commit_info() or {}
        operation = info.get("operation", "?")
        print(f"{commit.version:8d}  {operation:<18} {', '.join(commit.action_types())}")


def _print_commit(log: TableLog, version: int) -> None:
    try:
        commit = log.commit(version)
    except KeyError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)
    print(f"# {commit.path.name}")
    for action in commit.actions:
        print(json.dumps(action, indent=2, sort_keys=False))


def _print_snapshot(log: TableLog, version: int | None) -> None:
    snap = log.snapshot(version)
    payload: dict[str, Any] = {
        "version": snap.version,
        "from_checkpoint": snap.from_checkpoint,
        "protocol": snap.protocol,
        "metadata": {
            "id": snap.table_id,
            "partitionColumns": (snap.metadata or {}).get("partitionColumns"),
            "configuration": (snap.metadata or {}).get("configuration"),
            "schema": snap.schema,
        },
        "txns": snap.txns,
        "files": sorted(snap.files),
    }
    print(json.dumps(payload, indent=2))


def _print_files(log: TableLog, version: int | None) -> None:
    snap = log.snapshot(version)
    print(f"{'size':>10}  {'dataChange':<10}  path")
    for path, add in sorted(snap.files.items()):
        print(f"{add.get('size', 0):10d}  {str(add.get('dataChange')):<10}  {path}")


if __name__ == "__main__":
    raise SystemExit(main())
