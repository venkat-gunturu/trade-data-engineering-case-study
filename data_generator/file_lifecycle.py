"""File lifecycle for the trade pipeline: discover what is waiting, archive what is done.

    data/trades_batch_*.json          waiting to be processed
    data/archive/trades_batch_*.json  successfully processed

The pipeline is file-driven rather than batch-number-driven: it processes
whatever is sitting directly in data/ when it runs, and moves each file to
data/archive/ only after ingestion AND the dbt build have both succeeded. A file
that fails either step stays in data/, so the next run retries it.

Discovery is deliberately non-recursive. data/archive/ lives inside data/, and a
recursive scan would pick processed files up again on every run.

Used two ways:
    import  - the trade_pipeline DAG calls discover_files() to decide whether
              there is anything to do
    CLI     - the archive_files task runs this module as a script after dbt

Usage:
    python file_lifecycle.py --file ../data/trades_batch_001.json
"""

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
ARCHIVE_DIR = DATA_DIR / "archive"

# The generator writes this name shape and ingest_trades.py derives BATCH_NNN
# from it, so discovery matches the same pattern rather than every *.json.
BATCH_GLOB = "trades_batch_*.json"


def discover_files(data_dir=None):
    """Return the batch files waiting directly in data/, sorted by name.

    glob() rather than rglob(): anything already moved into data/archive/ must
    never be picked up again.
    """
    directory = Path(data_dir) if data_dir else DATA_DIR
    if not directory.is_dir():
        return []
    return sorted(path for path in directory.glob(BATCH_GLOB) if path.is_file())


def archive_files(paths, archive_dir=None):
    """Move processed files into data/archive/. Returns the new paths.

    Called only after ingestion and dbt have both succeeded. A file that is
    already gone is reported and skipped rather than failing the task - the work
    it represented was still completed.
    """
    destination = Path(archive_dir) if archive_dir else ARCHIVE_DIR
    destination.mkdir(parents=True, exist_ok=True)

    archived = []
    for path in paths:
        path = Path(path)
        if not path.is_file():
            print(f"  skipped {path.name}: no longer in data/")
            continue

        target = destination / path.name
        # replace() overwrites an identically named file from an earlier cycle,
        # which is what we want: archive/ holds the most recent copy.
        path.replace(target)
        print(f"  archived {path.name} -> {target.parent.name}/")
        archived.append(target)

    return archived


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--file",
        nargs="+",
        required=True,
        help="Files to archive. The DAG passes the same list it ingested.",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    print(f"Archiving {len(args.file)} file(s) to {ARCHIVE_DIR}")
    archived = archive_files(args.file)
    print(f"\nDone. {len(archived)} file(s) archived.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
