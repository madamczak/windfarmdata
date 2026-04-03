"""
Convert SQLite wind farm databases to Parquet files.

Rules:
- Kelmarsh status: only convert kelmarsh_status_by_turbine_duration (skip kelmarsh_status_by_turbine).
  The duration column in this DB is already fixed (calculated from next event).
- Kelmarsh data: convert kelmarsh_data_by_turbine — no duration fix needed.
- Penmanshiel status: fix the duration column by calculating it from the next event's timestamp.
- Penmanshiel data: convert penmanshiel_data_by_turbine — no duration fix needed.

Output:
  data/kelmarsh/status_<table>.parquet
  data/kelmarsh/data_<table>.parquet
  data/penmanshiel/status_<table>.parquet
  data/penmanshiel/data_<table>.parquet
"""

import sqlite3
import os
import pandas as pd

# ---------------------------------------------------------------------------
# Source database paths
# ---------------------------------------------------------------------------
BACKUP_DIR = r"C:\Users\adamc\PycharmProjects\data_by_turbine — backup"

KELMARSH_STATUS_DBS = {
    # kelmarsh_status_by_turbine is intentionally excluded — use duration version only
    "kelmarsh_status_by_turbine_duration": os.path.join(
        BACKUP_DIR, "kelmarsh_status_by_turbine_duration.db"
    ),
}

KELMARSH_DATA_DBS = {
    "kelmarsh_data_by_turbine": os.path.join(
        BACKUP_DIR, "kelmarsh_data_by_turbine.db"
    ),
}

PENMANSHIEL_STATUS_DBS = {
    "penmanshiel_status_by_turbine": os.path.join(
        BACKUP_DIR, "penmanshiel_status_by_turbine.db"
    ),
}

PENMANSHIEL_DATA_DBS = {
    "penmanshiel_data_by_turbine": os.path.join(
        BACKUP_DIR, "penmanshiel_data_by_turbine.db"
    ),
}

# ---------------------------------------------------------------------------
# Output directories (relative to this script's project root)
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
KELMARSH_OUT = os.path.join(PROJECT_ROOT, "data", "kelmarsh")
PENMANSHIEL_OUT = os.path.join(PROJECT_ROOT, "data", "penmanshiel")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_tables(conn: sqlite3.Connection) -> list[str]:
    """Return all table names in the given SQLite connection."""
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    return [row[0] for row in cur.fetchall()]


def load_table(conn: sqlite3.Connection, table: str) -> pd.DataFrame:
    """Load an entire SQLite table into a DataFrame."""
    return pd.read_sql_query(f'SELECT * FROM "{table}"', conn)


def fix_duration_column(df: pd.DataFrame, timestamp_col: str = "timestamp",
                        turbine_col: str = "turbine_id",
                        duration_col: str = "duration") -> pd.DataFrame:
    """
    Recalculate the duration column for status tables where duration was not
    pre-computed from the next event.

    Duration is defined as the number of seconds between the current event's
    timestamp and the next event's timestamp within the same turbine.

    For the last event of each turbine the duration is left as NaN (unknown end).

    The function is tolerant of different column name casings and will detect
    the timestamp / turbine / duration columns automatically if the exact names
    are not found.
    """

    # --- normalise column names for lookup (case-insensitive) ---
    col_map = {c.lower(): c for c in df.columns}

    def resolve(name: str) -> str | None:
        """Return the actual column name, trying lowercase match as fallback."""
        if name in df.columns:
            return name
        return col_map.get(name.lower())

    ts_col = resolve(timestamp_col) or resolve("timestamp") or resolve("time")
    tb_col = resolve(turbine_col) or resolve("turbine_id") or resolve("turbine")
    dur_col = resolve(duration_col) or resolve("duration")

    if ts_col is None:
        print("  [WARN] Cannot find timestamp column — skipping duration fix.")
        return df

    # Parse timestamps (handles both ISO strings and Unix integers)
    df = df.copy()
    df[ts_col] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")

    if tb_col:
        # Sort within each turbine by timestamp then compute diff to next row
        df = df.sort_values([tb_col, ts_col]).reset_index(drop=True)
        next_ts = df.groupby(tb_col)[ts_col].shift(-1)
    else:
        # No turbine column — sort globally
        df = df.sort_values(ts_col).reset_index(drop=True)
        next_ts = df[ts_col].shift(-1)

    calculated_duration = (next_ts - df[ts_col]).dt.total_seconds()

    if dur_col:
        print(f"  Replacing existing '{dur_col}' column with recalculated values.")
        df[dur_col] = calculated_duration
    else:
        print("  Adding new 'duration' column.")
        df["duration"] = calculated_duration

    return df


def save_parquet(df: pd.DataFrame, out_dir: str, filename: str) -> None:
    """Save DataFrame as a Parquet file, creating directories as needed."""
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, filename)
    df.to_parquet(path, index=False, engine="pyarrow")
    size_mb = os.path.getsize(path) / 1_048_576
    print(f"  Saved: {path}  ({size_mb:.1f} MB,  {len(df):,} rows)")


# ---------------------------------------------------------------------------
# Conversion routines
# ---------------------------------------------------------------------------

def convert_kelmarsh() -> None:
    """Convert Kelmarsh status database to Parquet (duration-fixed DB only)."""
    print("\n" + "=" * 60)
    print("KELMARSH — STATUS")
    print("=" * 60)

    for db_label, db_path in KELMARSH_STATUS_DBS.items():
        print(f"\nProcessing: {db_label}")
        conn = sqlite3.connect(db_path)
        tables = get_tables(conn)
        print(f"  Tables found: {tables}")

        for table in tables:
            print(f"\n  Table: {table}")
            df = load_table(conn, table)
            print(f"  Rows: {len(df):,}  Columns: {list(df.columns)}")

            # Duration column is already correct in this DB — no fix needed
            out_filename = f"status_{table}.parquet"
            save_parquet(df, KELMARSH_OUT, out_filename)

        conn.close()


def convert_penmanshiel() -> None:
    """Convert Penmanshiel status database to Parquet, fixing duration column."""
    print("\n" + "=" * 60)
    print("PENMANSHIEL — STATUS")
    print("=" * 60)

    for db_label, db_path in PENMANSHIEL_STATUS_DBS.items():
        print(f"\nProcessing: {db_label}")
        conn = sqlite3.connect(db_path)
        tables = get_tables(conn)
        print(f"  Tables found: {tables}")

        for table in tables:
            print(f"\n  Table: {table}")
            df = load_table(conn, table)
            print(f"  Rows: {len(df):,}  Columns: {list(df.columns)}")

            print("  Fixing duration column...")
            df = fix_duration_column(df)

            out_filename = f"status_{table}.parquet"
            save_parquet(df, PENMANSHIEL_OUT, out_filename)

        conn.close()


def convert_data_dbs() -> None:
    """Convert SCADA/data databases for both Kelmarsh and Penmanshiel to Parquet.

    No duration fix is applied — these are SCADA measurement tables, not status tables.
    """
    jobs = [
        ("KELMARSH — DATA", KELMARSH_DATA_DBS, KELMARSH_OUT),
        ("PENMANSHIEL — DATA", PENMANSHIEL_DATA_DBS, PENMANSHIEL_OUT),
    ]

    for label, dbs, out_dir in jobs:
        print("\n" + "=" * 60)
        print(label)
        print("=" * 60)

        for db_label, db_path in dbs.items():
            print(f"\nProcessing: {db_label}")
            conn = sqlite3.connect(db_path)
            tables = get_tables(conn)
            print(f"  Tables found: {tables}")

            for table in tables:
                print(f"\n  Table: {table}")
                df = load_table(conn, table)
                print(f"  Rows: {len(df):,}  Columns: {list(df.columns)}")

                out_filename = f"data_{table}.parquet"
                save_parquet(df, out_dir, out_filename)

            conn.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Wind farm SQLite → Parquet converter")
    print(f"Kelmarsh output  : {KELMARSH_OUT}")
    print(f"Penmanshiel output: {PENMANSHIEL_OUT}")

    convert_kelmarsh()
    convert_penmanshiel()
    convert_data_dbs()

    print("\nDone.")

