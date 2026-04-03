"""
Query service — uses DuckDB to run efficient analytical queries against
parquet files without loading entire datasets into memory.
"""

import os
import glob
import duckdb

# Candidate column names for the timestamp, tried in order (case-insensitive match)
TIMESTAMP_COLUMN_CANDIDATES = [
    "Timestamp", "timestamp", "time", "Time", "datetime", "DateTime", "date", "Date"
]


def _detect_timestamp_column(conn: duckdb.DuckDBPyConnection, parquet_glob: str) -> str | None:
    """Return the first matching timestamp column found in the parquet schema."""
    try:
        result = conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{parquet_glob}') LIMIT 0"
        ).fetchall()
        col_names = [row[0] for row in result]
        col_lower = {c.lower(): c for c in col_names}
        for candidate in TIMESTAMP_COLUMN_CANDIDATES:
            actual = col_lower.get(candidate.lower())
            if actual:
                return actual
    except Exception:
        pass
    return None


def get_time_range(farm_dir: str) -> tuple[object, object, str | None]:
    """Return (earliest, latest, timestamp_column) for all parquet files in farm_dir.

    Uses DuckDB to read only the min/max of the timestamp column across all files
    in a single query — efficient even for large datasets.

    Returns (None, None, None) if no parquet files are found or no timestamp
    column can be detected.
    """
    parquet_files = glob.glob(os.path.join(farm_dir, "*.parquet"))
    if not parquet_files:
        return None, None, None

    # DuckDB glob pattern — use forward slashes for cross-platform safety
    parquet_glob = os.path.join(farm_dir, "*.parquet").replace("\\", "/")

    conn = duckdb.connect()

    ts_col = _detect_timestamp_column(conn, parquet_glob)
    if ts_col is None:
        conn.close()
        return None, None, None

    try:
        row = conn.execute(
            f'SELECT MIN("{ts_col}"), MAX("{ts_col}") FROM read_parquet(?)',
            [parquet_glob],
        ).fetchone()
        earliest, latest = row if row else (None, None)
    except Exception:
        earliest, latest = None, None
    finally:
        conn.close()

    return earliest, latest, ts_col

