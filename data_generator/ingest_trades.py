"""Ingest generated trade JSON batches into Snowflake RAW.RAW_TRADES.

Flow, per batch file:

    local JSON file
        -> PUT       -> TRADE_DB.RAW.TRADE_STAGE
        -> COPY INTO -> TRADE_DB.RAW.RAW_TRADES

Airflow will call this script as a subprocess. The ingestion logic lives here
deliberately, not inside the DAG: Python owns ingestion, Airflow owns orchestration.

RAW.RAW_TRADES is append-only. The source payload is stored unflattened in a
VARIANT column; ingestion metadata (batch_id, source_file_name, ingested_at) is
added alongside it.

Usage:
    python ingest_trades.py                      # ingest every batch in data/
    python ingest_trades.py --batch 001          # ingest a single batch
    python ingest_trades.py --file ../data/x.json
    python ingest_trades.py --force              # reload files already loaded
"""

import argparse
import os
import sys
from pathlib import Path

import snowflake.connector


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
ENV_FILE = PROJECT_ROOT / ".env"

STAGE = "TRADE_DB.RAW.TRADE_STAGE"
TARGET_TABLE = "TRADE_DB.RAW.RAW_TRADES"
FILE_FORMAT = "TRADE_DB.RAW.JSON_FILE_FORMAT"

REQUIRED_ENV_VARS = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
)


class IngestionError(Exception):
    """A batch failed to load. Causes a non-zero exit so Airflow sees the failure."""


def load_env_file(path=ENV_FILE):
    """Load KEY=VALUE lines from a local .env, without overriding real env vars."""
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def connect():
    """Open a Snowflake connection using credentials from the environment."""
    missing = [name for name in REQUIRED_ENV_VARS if not os.environ.get(name)]
    if missing:
        raise IngestionError(
            "Missing required environment variables: " + ", ".join(missing)
        )

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )


def rows_as_dicts(cursor):
    """Return the cursor result as a list of dicts keyed by uppercase column name."""
    columns = [column[0].upper() for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def batch_id_for(local_file):
    """trades_batch_001.json -> BATCH_001 (matches the ids in trade_scenarios.py)."""
    stem = local_file.stem
    if "_" not in stem:
        raise IngestionError(f"Cannot derive a batch id from file name: {local_file.name}")
    return "BATCH_" + stem.rsplit("_", 1)[1]


def put_file(cursor, local_file):
    """Upload the local JSON file to the named stage. Returns the staged file name."""
    uri = local_file.resolve().as_posix()
    cursor.execute(f"PUT 'file://{uri}' @{STAGE} AUTO_COMPRESS = TRUE OVERWRITE = TRUE")

    results = rows_as_dicts(cursor)
    if not results:
        raise IngestionError(f"PUT returned no result for {local_file.name}")

    result = results[0]
    status = result.get("STATUS", "")
    if status not in ("UPLOADED", "SKIPPED"):
        raise IngestionError(
            f"PUT failed for {local_file.name}: {status} {result.get('MESSAGE', '')}"
        )

    staged_name = result["TARGET"]
    print(f"  PUT       {local_file.name} -> @{STAGE}/{staged_name} ({status})")
    return staged_name


def copy_into(cursor, staged_name, batch_id, force):
    """Load the staged file into RAW_TRADES. Returns the number of rows loaded."""
    sql = f"""
        COPY INTO {TARGET_TABLE} (payload, batch_id, source_file_name, ingested_at)
        FROM (
            SELECT
                $1,
                '{batch_id}',
                METADATA$FILENAME,
                CURRENT_TIMESTAMP()
            FROM @{STAGE}/{staged_name}
        )
        FILE_FORMAT = (FORMAT_NAME = {FILE_FORMAT})
        ON_ERROR = ABORT_STATEMENT
        FORCE = {'TRUE' if force else 'FALSE'}
    """
    cursor.execute(sql)
    results = rows_as_dicts(cursor)

    if not results:
        raise IngestionError(f"COPY INTO returned no result for {staged_name}")

    # Snowflake returns a single informational row when load history skips the file.
    if "ROWS_LOADED" not in results[0]:
        print(f"  COPY INTO skipped: {results[0].get('STATUS', 'already loaded')}")
        print("            (re-run with --force to load it again)")
        return 0

    rows_loaded = 0
    for result in results:
        errors_seen = result.get("ERRORS_SEEN") or 0
        if errors_seen:
            raise IngestionError(
                f"COPY INTO reported {errors_seen} error(s) for {result.get('FILE')}: "
                f"{result.get('FIRST_ERROR')}"
            )
        rows_loaded += result.get("ROWS_LOADED") or 0

    print(f"  COPY INTO {TARGET_TABLE}: {rows_loaded} row(s) loaded as {batch_id}")
    return rows_loaded


def ingest_file(cursor, local_file, force):
    """PUT then COPY INTO a single batch file."""
    if not local_file.is_file():
        raise IngestionError(f"Batch file not found: {local_file}")

    batch_id = batch_id_for(local_file)
    print(f"Ingesting {local_file.name} ({batch_id})")

    staged_name = put_file(cursor, local_file)
    return copy_into(cursor, staged_name, batch_id, force)


def resolve_files(args):
    """Work out which batch files this run should ingest."""
    if args.file:
        return [Path(args.file)]

    if args.batch:
        return [DATA_DIR / f"trades_batch_{args.batch}.json"]

    files = sorted(DATA_DIR.glob("trades_batch_*.json"))
    if not files:
        raise IngestionError(
            f"No batch files found in {DATA_DIR}. Run generate_trades.py first."
        )
    return files


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--batch", help="Batch number to ingest, e.g. 001")
    group.add_argument("--file", help="Path to a specific JSON batch file")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reload files even if Snowflake load history says they were already loaded",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    load_env_file()

    try:
        files = resolve_files(args)
        connection = connect()
    except IngestionError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1

    total_rows = 0
    try:
        with connection.cursor() as cursor:
            for local_file in files:
                total_rows += ingest_file(cursor, local_file, args.force)
    except (IngestionError, snowflake.connector.Error) as error:
        print(f"ERROR: ingestion failed: {error}", file=sys.stderr)
        return 1
    finally:
        connection.close()

    print(f"\nDone. {len(files)} file(s) processed, {total_rows} row(s) loaded.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
