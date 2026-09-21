from pathlib import Path

from delta_inspect.log import load_table

FIXTURE = Path(__file__).resolve().parents[1] / "examples" / "fixtures" / "txns"


def test_loads_commits_in_order():
    log = load_table(FIXTURE)
    assert [c.version for c in log.commits] == [0, 1, 2, 3, 4]
    assert log.commit(0).action_types() == ["commitInfo", "protocol", "metaData", "add"]
    assert log.commit(2).action_types() == ["commitInfo", "add", "txn"]


def test_snapshot_replays_add_and_remove():
    log = load_table(FIXTURE)

    v0 = log.snapshot(0)
    assert list(v0.files) == [
        "part-00000-aaaaaaaa-1111-2222-3333-444444444444.c000.snappy.parquet"
    ]
    assert v0.protocol == {"minReaderVersion": 1, "minWriterVersion": 2}
    assert v0.table_id == "af23c9d7-fff1-4a5a-a2c8-55c59bd782aa"

    v3 = log.snapshot(3)
    assert len(v3.files) == 4
    assert v3.txns == {"3ae45b72-24e1-865a-a211-34987ae02f2a": 1}

    latest = log.snapshot()
    assert latest.version == 4
    assert list(latest.files) == [
        "part-00000-eeeeeeee-1111-2222-3333-444444444444.c000.snappy.parquet"
    ]
    assert latest.txns == {"3ae45b72-24e1-865a-a211-34987ae02f2a": 1}


def test_streaming_txn_is_in_commit_json():
    log = load_table(FIXTURE)
    txn = next(a["txn"] for a in log.commit(3).actions if "txn" in a)
    assert txn["appId"] == "3ae45b72-24e1-865a-a211-34987ae02f2a"
    assert txn["version"] == 1


def test_optimize_marks_files_not_data_change():
    log = load_table(FIXTURE)
    commit = log.commit(4)
    adds = [a["add"] for a in commit.actions if "add" in a]
    removes = [a["remove"] for a in commit.actions if "remove" in a]
    assert adds[0]["dataChange"] is False
    assert all(r["dataChange"] is False for r in removes)
    assert len(removes) == 4
