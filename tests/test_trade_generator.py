"""Unit tests for the synthetic trade generator.

The point of these tests is not that the data looks realistic, but that the
mandatory business-rule scenarios are guaranteed to be present no matter what
seed is used, and that a seed reproduces a run exactly.
"""

import json
from datetime import date

import pytest

import generate_trades
from trade_scenarios import BATCH_IDS, TRADE_FIELDS, build_batches


AS_OF = date(2026, 8, 29)
RECORDS_PER_BATCH = 200
SEED = 42


@pytest.fixture(scope="module")
def batches():
    return build_batches(RECORDS_PER_BATCH, SEED, AS_OF)


def _state_before(batches, batch_id):
    """{trade_id: highest-version record} across every batch preceding batch_id."""
    state = {}
    for current in BATCH_IDS:
        if current == batch_id:
            break
        for trade in batches[current]:
            trade_id = trade["trade_id"]
            known = state.get(trade_id)
            if known is None or trade["trade_version"] >= known["trade_version"]:
                state[trade_id] = trade
    return state


def _classify(batches, batch_id):
    """Count how each record in the batch relates to what came before it."""
    before = _state_before(batches, batch_id)
    counts = {"new": 0, "higher_version": 0, "same_version": 0, "lower_version": 0}

    for trade in batches[batch_id]:
        known = before.get(trade["trade_id"])
        if known is None:
            counts["new"] += 1
        elif trade["trade_version"] > known["trade_version"]:
            counts["higher_version"] += 1
        elif trade["trade_version"] == known["trade_version"]:
            counts["same_version"] += 1
        else:
            counts["lower_version"] += 1

    return counts


# --- Structure -------------------------------------------------------------


def test_every_batch_holds_the_configured_number_of_records(batches):
    assert tuple(batches) == BATCH_IDS
    for batch_id, trades in batches.items():
        assert len(trades) == RECORDS_PER_BATCH, batch_id


def test_every_trade_carries_exactly_the_agreed_payload_fields(batches):
    for batch_id, trades in batches.items():
        for trade in trades:
            assert set(trade) == set(TRADE_FIELDS), batch_id


def test_trade_ids_are_unique_within_a_batch(batches):
    for batch_id, trades in batches.items():
        trade_ids = [trade["trade_id"] for trade in trades]
        assert len(set(trade_ids)) == len(trade_ids), batch_id


def test_each_batch_serialises_to_a_valid_json_array(batches):
    for trades in batches.values():
        reloaded = json.loads(json.dumps(trades))
        assert isinstance(reloaded, list)
        assert reloaded == trades


def test_currency_codes_fit_the_snowflake_column(batches):
    for trades in batches.values():
        for trade in trades:
            assert len(trade["currency"]) == 3


# --- Reproducibility -------------------------------------------------------


def test_the_same_seed_reproduces_identical_data():
    assert build_batches(50, 7, AS_OF) == build_batches(50, 7, AS_OF)


def test_a_different_seed_produces_different_data():
    assert build_batches(50, 7, AS_OF) != build_batches(50, 8, AS_OF)


# --- Business-rule scenarios ----------------------------------------------


def test_baseline_batch_is_all_new_trades_with_future_maturity(batches):
    trades = batches["BATCH_001"]
    assert {trade["trade_version"] for trade in trades} == {1}
    for trade in trades:
        assert date.fromisoformat(trade["maturity_date"]) > AS_OF


@pytest.mark.parametrize(
    "batch_id, scenario, expected",
    [
        ("BATCH_002", "higher_version", 15),
        ("BATCH_002", "same_version", 15),
        ("BATCH_003", "lower_version", 15),
        ("BATCH_003", "same_version", 10),
        ("BATCH_004", "higher_version", 20),
        ("BATCH_004", "same_version", 5),
    ],
)
def test_version_scenarios_are_generated_as_planned(batches, batch_id, scenario, expected):
    assert _classify(batches, batch_id)[scenario] == expected


@pytest.mark.parametrize(
    "batch_id, expected",
    [("BATCH_002", 5), ("BATCH_003", 10), ("BATCH_004", 5)],
)
def test_past_maturity_trades_are_present(batches, batch_id, expected):
    past = [
        trade
        for trade in batches[batch_id]
        if date.fromisoformat(trade["maturity_date"]) < AS_OF
    ]
    assert len(past) == expected


def test_lower_version_trades_reference_a_trade_that_really_reached_version_two(batches):
    before = _state_before(batches, "BATCH_003")

    stale = [
        trade
        for trade in batches["BATCH_003"]
        if trade["trade_id"] in before
        and trade["trade_version"] < before[trade["trade_id"]]["trade_version"]
    ]

    assert stale, "no lower-version trades were generated"
    for trade in stale:
        known_version = before[trade["trade_id"]]["trade_version"]
        assert known_version >= 2
        assert trade["trade_version"] == known_version - 1


def test_same_version_resends_change_the_economics(batches):
    """Rule 2 is only testable downstream if the replacement differs from the original."""
    checked = 0

    for batch_id in BATCH_IDS[1:]:
        before = _state_before(batches, batch_id)
        for trade in batches[batch_id]:
            known = before.get(trade["trade_id"])
            if known is None or known["trade_version"] != trade["trade_version"]:
                continue
            assert (trade["price"], trade["notional_amount"]) != (
                known["price"],
                known["notional_amount"],
            )
            checked += 1

    assert checked == 30  # 15 + 10 + 5 across batches 002-004


# --- One batch per invocation ---------------------------------------------

# The generator writes a single batch per run and tracks the cycle in
# data/.generation_progress. What matters is that the cycle advances, rolls
# over, and still produces exactly the content build_batches() produces.

# Small enough to keep the tests fast, large enough for BATCH_002/003's quotas.
CYCLE_RECORDS = 60


def _run(output_dir, records=CYCLE_RECORDS):
    """Invoke the generator CLI once against a temporary data directory."""
    return generate_trades.main(
        [
            "--output-dir",
            str(output_dir),
            "--records-per-batch",
            str(records),
            "--seed",
            str(SEED),
            "--as-of-date",
            AS_OF.isoformat(),
        ]
    )


def _batch_files(output_dir):
    return sorted(path.name for path in output_dir.glob("trades_batch_*.json"))


def test_each_invocation_writes_exactly_one_batch_and_the_cycle_rolls_over(tmp_path):
    progress = tmp_path / generate_trades.PROGRESS_FILENAME

    for position, batch_id in enumerate(BATCH_IDS, start=1):
        assert _run(tmp_path) == 0
        expected = generate_trades.batch_filename(batch_id)
        assert _batch_files(tmp_path)[-1] == expected
        assert len(_batch_files(tmp_path)) == position
        assert generate_trades.read_progress(progress) == [
            generate_trades.batch_filename(other) for other in BATCH_IDS[:position]
        ]

    # Fifth invocation starts the cycle again at BATCH_001.
    assert _run(tmp_path) == 0
    assert generate_trades.read_progress(progress) == [
        generate_trades.batch_filename(BATCH_IDS[0])
    ]


def test_a_full_cycle_reproduces_the_all_at_once_content(tmp_path):
    expected = build_batches(CYCLE_RECORDS, SEED, AS_OF)

    for batch_id in BATCH_IDS:
        _run(tmp_path)
        written = json.loads(
            (tmp_path / generate_trades.batch_filename(batch_id)).read_text(
                encoding="utf-8"
            )
        )
        assert written == expected[batch_id]


def test_a_new_cycle_clears_archive_but_leaves_unprocessed_files_alone(tmp_path):
    archive = tmp_path / generate_trades.ARCHIVE_DIRNAME
    archive.mkdir()
    (archive / "trades_batch_001.json").write_text("[]", encoding="utf-8")
    waiting = tmp_path / "trades_batch_004.json"
    waiting.write_text("[]", encoding="utf-8")

    _run(tmp_path)  # count 0 -> new cycle

    assert list(archive.iterdir()) == []
    assert waiting.is_file()  # still waiting for the pipeline, untouched


def test_a_missing_archive_directory_is_not_an_error(tmp_path):
    assert not (tmp_path / generate_trades.ARCHIVE_DIRNAME).exists()
    assert _run(tmp_path) == 0
    assert _batch_files(tmp_path) == ["trades_batch_001.json"]


@pytest.mark.parametrize(
    "progress, expected",
    [
        ("", "trades_batch_001.json"),          # empty file
        ("trades_batch_001.json\n", "trades_batch_002.json"),
        ("trades_batch_001.json\ntrades_batch_002.json\n", "trades_batch_003.json"),
        (
            "trades_batch_001.json\ntrades_batch_002.json\ntrades_batch_003.json\n",
            "trades_batch_004.json",
        ),
    ],
)
def test_a_partial_checkpoint_resumes_at_the_next_batch(tmp_path, progress, expected):
    (tmp_path / generate_trades.PROGRESS_FILENAME).write_text(
        progress, encoding="utf-8"
    )

    assert _run(tmp_path) == 0
    assert _batch_files(tmp_path) == [expected]
