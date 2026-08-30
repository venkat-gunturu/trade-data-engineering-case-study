"""Write one synthetic trade batch as a JSON file for the Snowflake loader to ingest.

    build_batches()  ->  data/trades_batch_NNN.json

The file name convention and the top-level JSON array are part of the contract
with ingest_trades.py: the loader derives BATCH_NNN from the file name, and the
stage file format uses STRIP_OUTER_ARRAY so each array element becomes one row
in RAW.RAW_TRADES.

ONE FILE PER INVOCATION. A local checkpoint, data/.generation_progress, records
which batches of the current four-batch cycle have already been written, one
file name per line. The invocation writes the next batch in BATCH_IDS, and after
the fourth it clears data/archive/ and starts the cycle again.

The checkpoint is a plain file rather than a table because the generator and
Airflow share the same mounted data/ directory - there is nothing to coordinate
across hosts.

Why the whole sequence is still built in memory: trade_scenarios carries state
between batches (a lower-version record in BATCH_003 exists only because
BATCH_002 bumped that trade to version 2). Rebuilding batches 1..N from the same
seed and anchor date and writing only the Nth reproduces exactly the content the
all-at-once run produced.

Usage:
    python generate_trades.py
    python generate_trades.py --records-per-batch 500 --seed 7
    python generate_trades.py --as-of-date 2026-06-01
"""

import argparse
import json
import sys
from datetime import date
from pathlib import Path

from trade_scenarios import (
    BATCH_IDS,
    DEFAULT_RECORDS_PER_BATCH,
    DEFAULT_SEED,
    build_batches,
)


OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data"

PROGRESS_FILENAME = ".generation_progress"
ARCHIVE_DIRNAME = "archive"


def batch_filename(batch_id):
    """BATCH_001 -> trades_batch_001.json, the name ingest_trades.py parses back."""
    return f"trades_batch_{batch_id.split('_')[1]}.json"


def read_progress(progress_file):
    """File names already written in the current cycle. Missing file means a fresh cycle."""
    if not progress_file.is_file():
        return []
    lines = progress_file.read_text(encoding="utf-8").splitlines()
    return [line.strip() for line in lines if line.strip()]


def append_progress(progress_file, filename):
    """Record a batch as done. Called only after the file is safely on disk."""
    progress_file.parent.mkdir(parents=True, exist_ok=True)
    with progress_file.open("a", encoding="utf-8") as file:
        file.write(f"{filename}\n")


def reset_progress(progress_file):
    """Start the four-batch cycle over."""
    progress_file.parent.mkdir(parents=True, exist_ok=True)
    progress_file.write_text("", encoding="utf-8")


def clear_archive(archive_dir):
    """Empty data/archive/ at the start of a cycle. Returns how many files were removed.

    archive/ holds files the pipeline has already processed, so clearing it at a
    cycle boundary discards nothing that is still waiting. Only files directly in
    archive/ are touched, and a missing directory is not an error.
    """
    if not archive_dir.is_dir():
        return 0

    removed = 0
    for path in sorted(archive_dir.iterdir()):
        if path.is_file():
            path.unlink()
            removed += 1
    return removed


def write_batch(output_dir, batch_id, trades):
    """Write one batch as a JSON array. Returns the path written."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / batch_filename(batch_id)

    with output_file.open("w", encoding="utf-8") as file:
        json.dump(trades, file, indent=2)

    print(f"Generated {output_file} ({len(trades)} trades)")
    return output_file


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--records-per-batch",
        type=int,
        default=DEFAULT_RECORDS_PER_BATCH,
        help=f"Trades per batch file (default: {DEFAULT_RECORDS_PER_BATCH})",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"Random seed, so a run can be reproduced (default: {DEFAULT_SEED})",
    )
    parser.add_argument(
        "--as-of-date",
        type=date.fromisoformat,
        default=date.today(),
        help="Anchor date (YYYY-MM-DD) all generated dates derive from (default: today)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_DIR,
        help="Where to write the batch file (default: the project's data/ directory)",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    output_dir = Path(args.output_dir)
    progress_file = output_dir / PROGRESS_FILENAME
    archive_dir = output_dir / ARCHIVE_DIRNAME

    done = read_progress(progress_file)

    # A completed cycle rolls over: archive/ is emptied and the checkpoint reset.
    # Anything above four entries is treated the same way, so a corrupted or
    # hand-edited checkpoint recovers instead of failing.
    if len(done) >= len(BATCH_IDS):
        print(f"Cycle complete ({len(done)} batches). Starting a new cycle.")
        reset_progress(progress_file)
        done = []

    if not done:
        removed = clear_archive(archive_dir)
        print(f"New cycle: cleared {removed} file(s) from {archive_dir}")

    batch_id = BATCH_IDS[len(done)]

    try:
        batches = build_batches(
            records_per_batch=args.records_per_batch,
            seed=args.seed,
            as_of_date=args.as_of_date,
        )
    except ValueError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1

    trades = batches[batch_id]
    output_file = write_batch(output_dir, batch_id, trades)
    append_progress(progress_file, output_file.name)

    position = len(done) + 1
    print(
        f"\n{batch_id} ({position} of {len(BATCH_IDS)} this cycle), "
        f"{len(trades)} trades, seed={args.seed}, "
        f"as-of={args.as_of_date.isoformat()}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
