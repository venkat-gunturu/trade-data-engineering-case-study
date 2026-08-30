"""Write synthetic trade batches as JSON files for the Snowflake loader to ingest.

    build_batches()  ->  data/trades_batch_NNN.json

The file name convention and the top-level JSON array are part of the contract
with ingest_trades.py: the loader derives BATCH_NNN from the file name, and the
stage file format uses STRIP_OUTER_ARRAY so each array element becomes one row
in RAW.RAW_TRADES.

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

from trade_scenarios import DEFAULT_RECORDS_PER_BATCH, DEFAULT_SEED, build_batches


OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data"


def write_batch(output_dir, batch_id, trades):
    """Write one batch as a JSON array. Returns the path written."""
    output_dir.mkdir(parents=True, exist_ok=True)

    batch_number = batch_id.split("_")[1]
    output_file = output_dir / f"trades_batch_{batch_number}.json"

    with output_file.open("w", encoding="utf-8") as file:
        json.dump(trades, file, indent=2)

    print(f"Generated {output_file} ({len(trades)} trades)")
    return output_file


def parse_args():
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
        help="Where to write the batch files (default: the project's data/ directory)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    try:
        batches = build_batches(
            records_per_batch=args.records_per_batch,
            seed=args.seed,
            as_of_date=args.as_of_date,
        )
    except ValueError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1

    for batch_id, trades in batches.items():
        write_batch(args.output_dir, batch_id, trades)

    total = sum(len(trades) for trades in batches.values())
    print(
        f"\n{len(batches)} batches, {total} trades, "
        f"seed={args.seed}, as-of={args.as_of_date.isoformat()}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
