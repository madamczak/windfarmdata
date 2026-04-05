"""
Query service — uses DuckDB to run efficient analytical queries against
parquet files without loading entire datasets into memory.
"""

import logging
import os
import glob
import re
from datetime import date
import duckdb
import pyarrow.parquet as pq

logger = logging.getLogger("windfarm.services.query_service")

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
    logger.debug("_detect_timestamp_column_from_schema: inspecting '%s'", parquet_path)
    try:
        schema = pq.read_schema(parquet_path)
        col_lower = {f.name.lower(): f.name for f in schema}
        for candidate in TIMESTAMP_COLUMN_CANDIDATES:
            actual = col_lower.get(candidate.lower())
            if actual:
                logger.debug(
                    "_detect_timestamp_column_from_schema: detected '%s' in '%s'",
                    actual, os.path.basename(parquet_path),
                )
                return actual
    except Exception as exc:
        logger.warning(
            "_detect_timestamp_column_from_schema: failed to read schema from '%s' — %s",
            parquet_path, exc,
        )
    logger.debug(
        "_detect_timestamp_column_from_schema: no timestamp column found in '%s'",
        os.path.basename(parquet_path),
    )
    return None


def get_time_range(farm_dir: str) -> tuple[object, object, str | None]:
    """Return (earliest, latest, timestamp_column) for all parquet files in farm_dir."""
    logger.info("get_time_range: scanning '%s'", farm_dir)
    parquet_files = sorted(glob.glob(os.path.join(farm_dir, "*.parquet")))
    if not parquet_files:
        logger.warning("get_time_range: no parquet files found in '%s'", farm_dir)
        return None, None, None

    logger.debug("get_time_range: found %d parquet file(s)", len(parquet_files))

    # --- Group files by their timestamp column name ---
    groups: dict[str, list[str]] = {}
    for path in parquet_files:
        ts_col = _detect_timestamp_column_from_schema(path)
        if ts_col:
            groups.setdefault(ts_col, []).append(path)

    if not groups:
        logger.warning(
            "get_time_range: no timestamp columns detected in any file under '%s'",
            farm_dir,
        )
        return None, None, None

    logger.debug(
        "get_time_range: timestamp groups found: %s",
        {k: len(v) for k, v in groups.items()},
    )

    conn = duckdb.connect()
    overall_earliest = None
    overall_latest = None
    detected_ts_col = None

    for ts_col, files in groups.items():
        logger.debug(
            "get_time_range: querying group ts_col='%s' files=%d", ts_col, len(files)
        )
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
                logger.debug(
                    "get_time_range: ts_col='%s' earliest=%s latest=%s",
                    ts_col, earliest, latest,
                )
                if overall_earliest is None or earliest < overall_earliest:
                    overall_earliest = earliest
                if overall_latest is None or latest > overall_latest:
                    overall_latest = latest
                if detected_ts_col is None or len(files) > len(groups.get(detected_ts_col, [])):
                    detected_ts_col = ts_col
            else:
                logger.debug("get_time_range: ts_col='%s' — query returned no data", ts_col)
        except Exception as exc:
            logger.error(
                "get_time_range: DuckDB query failed for ts_col='%s' — %s", ts_col, exc
            )
            continue

    conn.close()
    logger.info(
        "get_time_range: result for '%s' — ts_col='%s' earliest=%s latest=%s",
        farm_dir, detected_ts_col, overall_earliest, overall_latest,
    )
    return overall_earliest, overall_latest, detected_ts_col


def get_columns_by_file_type(farm_dir: str) -> dict[str, list[str]]:
    """Return column names grouped by file type for all parquet files in farm_dir."""
    logger.info("get_columns_by_file_type: scanning '%s'", farm_dir)
    parquet_files = sorted(glob.glob(os.path.join(farm_dir, "*.parquet")))
    if not parquet_files:
        logger.warning("get_columns_by_file_type: no parquet files in '%s'", farm_dir)
        return {}

    logger.debug("get_columns_by_file_type: found %d file(s)", len(parquet_files))

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
                logger.debug(
                    "get_columns_by_file_type: registered file_type='%s' from '%s'",
                    file_type, filename,
                )
            continue

        # Convention 2: T{NN}_{SensorType}.parquet (Hill of Towie)
        m = re.match(r"^T\d+_([A-Za-z]+)\.parquet$", filename)
        if m:
            file_type = m.group(1)
            if file_type not in type_to_file:
                type_to_file[file_type] = path
                logger.debug(
                    "get_columns_by_file_type: registered file_type='%s' from '%s'",
                    file_type, filename,
                )

    result: dict[str, list[str]] = {}
    for file_type, path in sorted(type_to_file.items()):
        try:
            schema = pq.read_schema(path)
            cols = [field.name for field in schema]
            result[file_type] = cols
            logger.debug(
                "get_columns_by_file_type: file_type='%s' → %d column(s)",
                file_type, len(cols),
            )
        except Exception as exc:
            logger.error(
                "get_columns_by_file_type: failed to read schema for file_type='%s' path='%s' — %s",
                file_type, path, exc,
            )
            result[file_type] = []

    logger.info(
        "get_columns_by_file_type: done — %d file type(s) discovered in '%s'",
        len(result), farm_dir,
    )
    return result


def _files_for_type(farm_dir: str, file_type: str) -> list[str]:
    """Return all parquet file paths that belong to the given file_type."""
    pattern_scada = os.path.join(farm_dir, f"{file_type}_turbine_*.parquet")
    pattern_hot = os.path.join(farm_dir, f"T*_{file_type}.parquet")
    files = sorted(glob.glob(pattern_scada) + glob.glob(pattern_hot))
    logger.debug(
        "_files_for_type: file_type='%s' → %d file(s) matched in '%s'",
        file_type, len(files), farm_dir,
    )
    return files


def get_data_for_date(
    farm_dir: str,
    file_type: str,
    query_date: date,
    columns: list[str] | None = None,
) -> tuple[list[str], list[list]]:
    """Query all parquet files of a given file_type for a single calendar day."""
    logger.info(
        "get_data_for_date: farm_dir='%s' file_type='%s' date=%s columns=%s",
        farm_dir, file_type, query_date,
        columns if columns else "(all)",
    )

    files = _files_for_type(farm_dir, file_type)
    if not files:
        logger.warning(
            "get_data_for_date: no files found for file_type='%s' in '%s'",
            file_type, farm_dir,
        )
        raise ValueError(
            f"No parquet files found for file_type '{file_type}' in {farm_dir}"
        )

    logger.debug("get_data_for_date: %d file(s) will be queried", len(files))

    # Detect timestamp column from the first file in the group
    ts_col = _detect_timestamp_column_from_schema(files[0])
    if ts_col is None:
        logger.error(
            "get_data_for_date: cannot detect timestamp column in '%s'", files[0]
        )
        raise ValueError(
            f"Cannot detect a timestamp column in {files[0]}"
        )

    logger.debug("get_data_for_date: using timestamp column '%s'", ts_col)

    # Build the DuckDB file list
    file_list = ", ".join(
        f"'{p.replace(chr(92), '/')}'" for p in files
    )

    # Build column projection — always include the timestamp column
    if columns:
        selected = list(dict.fromkeys([ts_col] + columns))  # deduplicate, preserve order
        col_sql = ", ".join(f'"{c}"' for c in selected)
        logger.debug(
            "get_data_for_date: column projection — %d column(s): %s",
            len(selected), selected,
        )
    else:
        col_sql = "*"
        logger.debug("get_data_for_date: returning all columns (*)")

    # Date filter: cast the timestamp column to DATE and compare
    date_str = query_date.isoformat()
    query = (
        f"SELECT {col_sql} "
        f"FROM read_parquet([{file_list}], union_by_name=true) "
        f'WHERE CAST("{ts_col}" AS DATE) = \'{date_str}\' '
        f'ORDER BY "{ts_col}"'
    )
    logger.debug("get_data_for_date: executing DuckDB query for date=%s", date_str)

    conn = duckdb.connect()
    try:
        rel = conn.execute(query)
        col_names = [desc[0] for desc in rel.description]
        rows = rel.fetchall()
        logger.debug(
            "get_data_for_date: query complete — %d row(s) fetched, %d column(s)",
            len(rows), len(col_names),
        )
    except Exception as exc:
        logger.error(
            "get_data_for_date: DuckDB query failed for file_type='%s' date=%s — %s",
            file_type, date_str, exc,
        )
        raise
    finally:
        conn.close()

    # Convert each row to a plain list (handles datetime, Decimal, etc.)
    serialisable_rows = [list(row) for row in rows]
    logger.info(
        "get_data_for_date: returning %d row(s) for file_type='%s' date=%s",
        len(serialisable_rows), file_type, query_date,
    )
    return col_names, serialisable_rows

