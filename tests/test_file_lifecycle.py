"""Tests for the data/ -> archive/ file lifecycle.

The pipeline is file-driven: it processes whatever is waiting in data/ and moves
each file to data/archive/ only once ingestion and dbt have both succeeded. These
tests cover the two halves of that contract - what discovery returns, and what
archiving moves - plus the properties the design depends on.

No Snowflake, no Airflow: everything here runs against a temporary directory.
"""

import argparse
from pathlib import Path

import pytest

import file_lifecycle


def make_batches(directory, *names):
    directory.mkdir(parents=True, exist_ok=True)
    for name in names:
        (directory / name).write_text("[]", encoding="utf-8")
    return directory


# --- discovery -------------------------------------------------------------


def test_no_files_available_is_an_empty_result_not_an_error(tmp_path):
    """An empty data/ short-circuits the DAG into a clean success."""
    make_batches(tmp_path)
    assert file_lifecycle.discover_files(tmp_path) == []


def test_a_missing_data_directory_is_also_empty(tmp_path):
    assert file_lifecycle.discover_files(tmp_path / "not_created_yet") == []


def test_one_available_file_is_selected(tmp_path):
    make_batches(tmp_path, "trades_batch_001.json")
    found = file_lifecycle.discover_files(tmp_path)
    assert [path.name for path in found] == ["trades_batch_001.json"]


def test_multiple_files_are_all_discovered_in_order(tmp_path):
    make_batches(tmp_path, "trades_batch_003.json", "trades_batch_001.json",
                 "trades_batch_002.json")
    found = file_lifecycle.discover_files(tmp_path)
    assert [path.name for path in found] == [
        "trades_batch_001.json",
        "trades_batch_002.json",
        "trades_batch_003.json",
    ]


def test_archived_files_are_never_rediscovered(tmp_path):
    """The reason discovery uses glob and not rglob: archive/ lives inside data/."""
    make_batches(tmp_path, "trades_batch_001.json")
    make_batches(tmp_path / "archive", "trades_batch_002.json",
                 "trades_batch_003.json")

    found = file_lifecycle.discover_files(tmp_path)

    assert [path.name for path in found] == ["trades_batch_001.json"]


def test_unrelated_files_are_ignored(tmp_path):
    make_batches(tmp_path, "trades_batch_001.json")
    (tmp_path / "notes.txt").write_text("x", encoding="utf-8")
    (tmp_path / "trades_batch_002.json.tmp").write_text("x", encoding="utf-8")

    found = file_lifecycle.discover_files(tmp_path)

    assert [path.name for path in found] == ["trades_batch_001.json"]


# --- archiving -------------------------------------------------------------


def test_successful_processing_moves_files_to_archive(tmp_path):
    data = make_batches(tmp_path, "trades_batch_001.json", "trades_batch_002.json")
    archive = tmp_path / "archive"

    archived = file_lifecycle.archive_files(sorted(data.glob("*.json")), archive)

    assert sorted(p.name for p in archived) == [
        "trades_batch_001.json",
        "trades_batch_002.json",
    ]
    assert file_lifecycle.discover_files(data) == [], "data/ should be empty now"
    assert sorted(p.name for p in archive.glob("*.json")) == [
        "trades_batch_001.json",
        "trades_batch_002.json",
    ]


def test_the_archive_directory_is_created_on_demand(tmp_path):
    data = make_batches(tmp_path, "trades_batch_001.json")
    archive = tmp_path / "archive"
    assert not archive.exists()

    file_lifecycle.archive_files([data / "trades_batch_001.json"], archive)

    assert (archive / "trades_batch_001.json").is_file()


def test_only_the_named_files_are_archived(tmp_path):
    """A file generated mid-run stays in data/ for the next run."""
    data = make_batches(tmp_path, "trades_batch_001.json", "trades_batch_002.json")
    archive = tmp_path / "archive"

    file_lifecycle.archive_files([data / "trades_batch_001.json"], archive)

    assert [p.name for p in file_lifecycle.discover_files(data)] == [
        "trades_batch_002.json"
    ]


def test_a_file_that_vanished_is_skipped_not_fatal(tmp_path):
    archive = tmp_path / "archive"
    archived = file_lifecycle.archive_files([tmp_path / "gone.json"], archive)
    assert archived == []


def test_failed_processing_leaves_the_file_in_data(tmp_path):
    """Ingestion or dbt failure means archive_files never runs, so data/ keeps the file.

    The DAG expresses this with task ordering - archive_files is downstream of
    both ingest_trades and dbt_build - so the check here is that nothing else
    moves the file on its own.
    """
    data = make_batches(tmp_path, "trades_batch_001.json")

    # discovery is read-only: calling it must not move or delete anything
    file_lifecycle.discover_files(data)

    assert (data / "trades_batch_001.json").is_file()
    assert not (data / "archive").exists()


def test_archive_cli_requires_explicit_files():
    with pytest.raises(SystemExit):
        file_lifecycle.parse_args([])

    args = file_lifecycle.parse_args(["--file", "a.json", "b.json"])
    assert args.file == ["a.json", "b.json"]


# --- properties the refactor depends on ------------------------------------


class _RecordingCursor:
    """Stand-in for a Snowflake cursor: records the SQL, returns canned rows.

    Only the three members ingest_trades actually touches are implemented, so
    these tests assert on the statements the loader really issues rather than on
    its source text - a docstring cannot satisfy them.
    """

    def __init__(self):
        self.statements = []
        self.description = []
        self._rows = []

    def execute(self, sql):
        self.statements.append(sql)
        if sql.lstrip().upper().startswith("PUT"):
            self.description = [("source",), ("target",), ("status",)]
            self._rows = [
                ("trades_batch_001.json", "trades_batch_001.json.gz", "UPLOADED")
            ]
        elif "COPY INTO" in sql.upper():
            self.description = [("file",), ("rows_loaded",), ("errors_seen",)]
            self._rows = [("trades_batch_001.json.gz", 3, 0)]
        else:
            self.description = [("status",)]
            self._rows = [("ok",)]

    def fetchall(self):
        return self._rows


def _statements_matching(cursor, keyword):
    return [sql for sql in cursor.statements if keyword in sql.upper()]


def test_put_overwrites_the_staged_file():
    """Batch file names repeat every cycle, so the upload has to replace.

    Without OVERWRITE, PUT reports SKIPPED for a name already on the stage and
    COPY INTO would load the previous cycle's bytes.
    """
    import ingest_trades

    cursor = _RecordingCursor()
    ingest_trades.put_file(cursor, Path("data") / "trades_batch_001.json")

    puts = _statements_matching(cursor, "PUT ")
    assert len(puts) == 1, "put_file should issue exactly one PUT"
    assert "OVERWRITE = TRUE" in puts[0].upper()


def test_the_staged_file_is_kept_after_a_successful_load(tmp_path):
    """Staged files are retained for visibility, so nothing may clear them."""
    import ingest_trades

    batch = tmp_path / "trades_batch_001.json"
    batch.write_text("[]", encoding="utf-8")

    cursor = _RecordingCursor()
    assert ingest_trades.ingest_file(cursor, batch, force=False) == 3

    assert _statements_matching(cursor, "REMOVE") == [], "no REMOVE may be issued"


def test_a_loaded_file_is_recorded_in_the_control_table(tmp_path):
    """One LOAD_CONTROL row per file, keyed by the staged name RAW_TRADES carries."""
    import ingest_trades

    batch = tmp_path / "trades_batch_001.json"
    batch.write_text("[]", encoding="utf-8")

    cursor = _RecordingCursor()
    ingest_trades.ingest_file(cursor, batch, force=False)

    inserts = _statements_matching(cursor, "INSERT INTO")
    assert len(inserts) == 1, "exactly one control row per file"
    statement = inserts[0].upper()
    assert "LOAD_CONTROL" in statement
    assert "'TRADES_BATCH_001.JSON.GZ'" in statement
    assert "'LOADED'" in statement


def test_the_loader_defaults_to_whatever_is_waiting_in_data(monkeypatch, tmp_path):
    """No --batch, no --file: the file-driven path the DAG relies on."""
    import ingest_trades

    make_batches(tmp_path, "trades_batch_007.json", "trades_batch_009.json")
    monkeypatch.setattr(ingest_trades, "DATA_DIR", tmp_path)

    files = ingest_trades.resolve_files(argparse.Namespace(file=None, batch=None))

    assert [path.name for path in files] == [
        "trades_batch_007.json",
        "trades_batch_009.json",
    ]


def test_the_loader_accepts_several_files_at_once(tmp_path):
    import ingest_trades

    args = argparse.Namespace(
        file=[str(tmp_path / "a.json"), str(tmp_path / "b.json")], batch=None
    )
    assert [Path(p).name for p in ingest_trades.resolve_files(args)] == [
        "a.json",
        "b.json",
    ]


def test_the_dag_has_no_hardcoded_batch_number():
    """The pipeline reacts to files; it must not name a batch."""
    dag = Path(__file__).resolve().parent.parent / "airflow" / "dags" / "trade_pipeline.py"
    source = dag.read_text(encoding="utf-8")

    assert "BATCH_001" not in source
    assert "--batch" not in source
    assert 'Param(\n            "001"' not in source
    assert "discover_files" in source, "the DAG must drive off file discovery"


def test_the_graph_is_only_the_tasks_that_do_work():
    """Four tasks, no notification task - notification is configuration."""
    dag = Path(__file__).resolve().parent.parent / "airflow" / "dags" / "trade_pipeline.py"
    source = dag.read_text(encoding="utf-8")

    assert "find_files >> ingest_trades >> dbt_build >> archive_files" in source
    assert "notify_failure" not in source
