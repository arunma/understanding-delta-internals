from pathlib import Path

from delta_inspect.cli import main

FIXTURE = Path(__file__).resolve().parents[1] / "examples" / "fixtures" / "txns"


def test_cli_summary(capsys):
    assert main([str(FIXTURE), "summary"]) == 0
    out = capsys.readouterr().out
    assert "commits:    5" in out
    assert "live files: 1" in out
    assert "3ae45b72-24e1-865a-a211-34987ae02f2a: 1" in out


def test_cli_history(capsys):
    assert main([str(FIXTURE), "history"]) == 0
    out = capsys.readouterr().out
    assert "CREATE TABLE" in out
    assert "STREAMING UPDATE" in out
    assert "OPTIMIZE" in out
