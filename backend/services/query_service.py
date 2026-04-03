"""
Query service — uses DuckDB to run efficient analytical queries against
parquet files without loading entire datasets into memory.
"""

import os
import glob
import duckdb
import pyarrow.parquet as pq

# Candidate column names for the timestamp, tried in order (case-insensitive match)
TIMESTAMP_COLUMN_CANDIDATES = [
    "Date and time",  # Kelmarsh / Penmanshiel data files
    "TimeStamp",      # Hill of Towie SCADA files
    "timestamp",
    "Timestamp",
    "time",
    "Time",
    "datetime",
    "DateTime",
    "date",
    "Date",
]


def _detect_timestamp_column_from_schema(parquet_path: str) -> str | None:
    """Detect timestamp column by reading the parquet file schema via PyArrow.

    Reading the schema is extremely fast — it only reads file metadata, not data.
    """
    try:
        schema = pq.read_schema(parquet_path)
        col_lower = {f.name.lower(): f.name for f in schema}
        for candidate in TIMESTAMP_COLUMN_CANDIDATES:
            actual = col_lower.get(candidate.lower())
            if actual:
                return actual
    except Exception:
        pass
    return None


def get_time_range(farm_dir: str) -> tuple[object, object, str | None]:
    """Return (earliest, latest, timestamp_column) for all parquet files in farm_dir.

    Strategy:
    - Groups parquet files by their detected timestamp column name so that only
      files sharing the same schema are queried together.
    - Uses DuckDB with union_by_name=True to safely handle minor schema differences
      within the same group.
    - Picks the overall min/max across all groups.

    Returns (None, None, None) if no parquet files are found or no timestamp
    column can be detected.
    """
    parquet_files = sorted(glob.glob(os.path.join(farm_dir, "*.parquet")))
    if not parquet_files:
        return None, None, None

    # --- Group files by their timestamp column name ---
    groups: dict[str, list[str]] = {}
    for path in parquet_files:
        ts_col = _detect_timestamp_column_from_schema(path)
        if ts_col:
            groups.setdefault(ts_col, []).append(path)

    if not groups:
        return None, None, None

    conn = duckdb.connect()
    overall_earliest = None
    overall_latest = None
    detected_ts_col = None

    for ts_col, files in groups.items():
        # Build a quoted list of file paths for DuckDB
        file_list = ", ".join(f"'{p.replace(chr(92), '/')}'" for p in files)
        query = (
            f'SELECT MIN("{ts_col}"), MAX("{ts_col}") '
            f"FROM read_parquet([{file_list}], union_by_name=true)"
        )
        try:
            row = conn.execute(query).fetchone()
            if row and row[0] is not None:
                earliest, latest = row
                if overall_earliest is None or earliest < overall_earliest:
                    overall_earliest = earliest
                if overall_latest is None or latest > overall_latest:
                    overall_latest = latest
                # Use the ts_col from the largest group as the "primary" one
                if detected_ts_col is None or len(files) > len(groups.get(detected_ts_col, [])):
                    detected_ts_col = ts_col
        except Exception:
            continue

    conn.close()
    return overall_earliest, overall_latest, detected_ts_col
