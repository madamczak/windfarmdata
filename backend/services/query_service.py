"""
Query service — uses DuckDB to run efficient analytical queries against
parquet files without loading entire datasets into memory.
"""

import os
import glob
import re
from datetime import date
import duckdb
import pyarrow.parquet as pq

# Candidate column names for the timestamp, tried in order (case-insensitive match)
TIMESTAMP_COLUMN_CANDIDATES = [
    "Date and time",    # Kelmarsh / Penmanshiel data files
    "Timestamp start",  # Kelmarsh / Penmanshiel status files
    "Timestamp end",    # Kelmarsh / Penmanshiel status files (fallback)
    "TimeStamp",        # Hill of Towie SCADA files
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


def get_columns_by_file_type(farm_dir: str) -> dict[str, list[str]]:
    """Return column names grouped by file type for all parquet files in farm_dir.

    Supports two naming conventions:

    Kelmarsh / Penmanshiel:
        data_turbine_N.parquet   → file type "data"
        status_turbine_N.parquet → file type "status"

    Hill of Towie:
        T{NN}_{SensorType}.parquet  → file type is the sensor type suffix,
        e.g. T01_SCTurbine.parquet  → "SCTurbine"
             T01_AlarmLog.parquet   → "AlarmLog"

    Only the schema of the first file encountered for each type is read (all
    turbines share the same schema), so this is fast — no row data is loaded.

    Returns a dict mapping file_type → list of column names.
    Empty dict if the directory contains no parquet files or no recognised
    naming patterns are found.
    """
    parquet_files = sorted(glob.glob(os.path.join(farm_dir, "*.parquet")))
    if not parquet_files:
        return {}

    # Map file_type → first matching file path
    type_to_file: dict[str, str] = {}
    for path in parquet_files:
        filename = os.path.basename(path)

        # Convention 1: data_turbine_N.parquet / status_turbine_N.parquet
        m = re.match(r"^([a-zA-Z]+)_turbine_\d+\.parquet$", filename)
        if m:
            file_type = m.group(1)
            if file_type not in type_to_file:
                type_to_file[file_type] = path
            continue

        # Convention 2: T{NN}_{SensorType}.parquet (Hill of Towie)
        m = re.match(r"^T\d+_([A-Za-z]+)\.parquet$", filename)
        if m:
            file_type = m.group(1)
            if file_type not in type_to_file:
                type_to_file[file_type] = path

    result: dict[str, list[str]] = {}
    for file_type, path in sorted(type_to_file.items()):
        try:
            schema = pq.read_schema(path)
            result[file_type] = [field.name for field in schema]
        except Exception:
            result[file_type] = []

    return result


def _files_for_type(farm_dir: str, file_type: str) -> list[str]:
    """Return all parquet file paths that belong to the given file_type.

    Supports both naming conventions:
      - Kelmarsh/Penmanshiel: {file_type}_turbine_N.parquet
      - Hill of Towie:        T{NN}_{file_type}.parquet
    """
    pattern_scada = os.path.join(farm_dir, f"{file_type}_turbine_*.parquet")
    pattern_hot = os.path.join(farm_dir, f"T*_{file_type}.parquet")
    files = sorted(glob.glob(pattern_scada) + glob.glob(pattern_hot))
    return files


def get_data_for_date(
    farm_dir: str,
    file_type: str,
    query_date: date,
    columns: list[str] | None = None,
) -> tuple[list[str], list[list]]:
    """Query all parquet files of a given file_type for a single calendar day.

    Parameters
    ----------
    farm_dir:    Absolute path to the farm's parquet directory.
    file_type:   File-type group to query (e.g. "data", "status", "SCTurbine").
    query_date:  The calendar day to filter on.
    columns:     Explicit list of column names to return.  When None or empty
                 all columns are returned.

    Returns
    -------
    A tuple of (column_names, rows) where rows is a list of lists.
    Raises ValueError if no matching files are found or the timestamp column
    cannot be detected.
    """
    files = _files_for_type(farm_dir, file_type)
    if not files:
        raise ValueError(
            f"No parquet files found for file_type '{file_type}' in {farm_dir}"
        )

    # Detect timestamp column from the first file in the group
    ts_col = _detect_timestamp_column_from_schema(files[0])
    if ts_col is None:
        raise ValueError(
            f"Cannot detect a timestamp column in {files[0]}"
        )

    # Build the DuckDB file list
    file_list = ", ".join(
        f"'{p.replace(chr(92), '/')}'" for p in files
    )

    # Build column projection — always include the timestamp column
    if columns:
        # Ensure the timestamp column is always present in the projection
        selected = list(dict.fromkeys([ts_col] + columns))  # deduplicate, preserve order
        col_sql = ", ".join(f'"{c}"' for c in selected)
    else:
        col_sql = "*"

    # Date filter: cast the timestamp column to DATE and compare
    date_str = query_date.isoformat()  # "YYYY-MM-DD"
    query = (
        f"SELECT {col_sql} "
        f"FROM read_parquet([{file_list}], union_by_name=true) "
        f'WHERE CAST("{ts_col}" AS DATE) = \'{date_str}\' '
        f'ORDER BY "{ts_col}"'
    )

    conn = duckdb.connect()
    try:
        rel = conn.execute(query)
        col_names = [desc[0] for desc in rel.description]
        rows = rel.fetchall()
    finally:
        conn.close()

    # Convert each row to a plain list (handles datetime, Decimal, etc.)
    serialisable_rows = [list(row) for row in rows]
    return col_names, serialisable_rows


