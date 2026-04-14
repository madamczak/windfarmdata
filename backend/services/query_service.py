"""
Query service — uses DuckDB to run efficient analytical queries against
parquet files without loading entire datasets into memory.

Supports two storage backends:
  local  — parquet files are on the local filesystem; paths are local paths.
  r2     — parquet files live in Cloudflare R2; paths are s3:// URLs.
           DuckDB's httpfs extension is used for direct access — no downloads.
"""

import logging
import os
import glob
import re
from datetime import date
import duckdb
import pyarrow.parquet as pq

from backend.config import settings

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


def _is_s3_path(path: str) -> bool:
    """Return True if *path* is an S3 URL (s3://)."""
    return path.startswith("s3://")


def _make_duckdb_conn():
    """Create a DuckDB in-memory connection, configuring httpfs when in R2 mode."""
    conn = duckdb.connect()
    if settings.storage_backend == "r2":
        import backend.services.r2_service as _r2
        _r2.configure_s3_duckdb(conn)
    return conn


def _detect_timestamp_column_from_schema(parquet_path: str) -> str | None:
    """Detect timestamp column by reading the parquet file schema.

    For local files: uses PyArrow (reads metadata only, very fast).
    For S3 URLs: uses DuckDB DESCRIBE (reads parquet footer over network).
    """
    logger.debug("_detect_timestamp_column_from_schema: inspecting '%s'", parquet_path)

    if _is_s3_path(parquet_path):
        return _detect_timestamp_column_via_duckdb(parquet_path)

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


def _detect_timestamp_column_via_duckdb(parquet_path: str) -> str | None:
    """Detect the timestamp column from an S3 parquet URL using DuckDB DESCRIBE.

    This reads only the parquet footer (metadata) — no row data is transferred.
    """
    try:
        conn = _make_duckdb_conn()
        rows = conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')"
        ).fetchall()
        conn.close()
        col_lower = {row[0].lower(): row[0] for row in rows}
        for candidate in TIMESTAMP_COLUMN_CANDIDATES:
            actual = col_lower.get(candidate.lower())
            if actual:
                logger.debug(
                    "_detect_timestamp_column_via_duckdb: detected '%s' in '%s'",
                    actual, parquet_path,
                )
                return actual
    except Exception as exc:
        logger.warning(
            "_detect_timestamp_column_via_duckdb: failed for '%s' — %s",
            parquet_path, exc,
        )
    return None


def _get_schema_columns(parquet_path: str) -> list[str]:
    """Return all column names for a parquet file (local or S3)."""
    if _is_s3_path(parquet_path):
        try:
            conn = _make_duckdb_conn()
            rows = conn.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')"
            ).fetchall()
            conn.close()
            return [row[0] for row in rows]
        except Exception as exc:
            logger.warning("_get_schema_columns: DuckDB DESCRIBE failed for '%s' — %s", parquet_path, exc)
            return []
    else:
        try:
            schema = pq.read_schema(parquet_path)
            return [field.name for field in schema]
        except Exception as exc:
            logger.warning("_get_schema_columns: PyArrow failed for '%s' — %s", parquet_path, exc)
            return []


def _list_farm_parquet_files(farm_dir_or_prefix: str) -> list[str]:
    """List parquet files in a farm directory (local) or S3 prefix (R2).

    For local: returns sorted absolute paths via glob.
    For R2: uses r2_service.list_farm_files — boto3 metadata listing only.
    """
    if _is_s3_path(farm_dir_or_prefix):
        import backend.services.r2_service as _r2
        # Extract farm name from s3://bucket/farm/
        parts = farm_dir_or_prefix.rstrip("/").split("/")
        farm = parts[-1]
        return _r2.list_farm_files(farm)
    return sorted(glob.glob(os.path.join(farm_dir_or_prefix, "*.parquet")))


def _basename(path: str) -> str:
    """Return the filename portion of a local path or S3 URL."""
    # S3 URLs always use forward slashes; local paths may use backslash on Windows.
    # posixpath / os.path.basename both handle the cases correctly together.
    if path.startswith("s3://"):
        return path.rstrip("/").split("/")[-1]
    return os.path.basename(path)


def get_time_range(farm_dir: str) -> tuple[object, object, str | None]:
    """Return (earliest, latest, timestamp_column) for all parquet files in farm_dir."""
    logger.info("get_time_range: scanning '%s'", farm_dir)
    parquet_files = _list_farm_parquet_files(farm_dir)
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

    conn = _make_duckdb_conn()
    overall_earliest = None
    overall_latest = None
    detected_ts_col = None

    for ts_col, files in groups.items():
        logger.debug(
            "get_time_range: querying group ts_col='%s' files=%d", ts_col, len(files)
        )
        # Build a quoted list of file paths / S3 URLs for DuckDB
        file_list = ", ".join(f"'{p}'" for p in files)
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
    parquet_files = _list_farm_parquet_files(farm_dir)
    if not parquet_files:
        logger.warning("get_columns_by_file_type: no parquet files in '%s'", farm_dir)
        return {}

    logger.debug("get_columns_by_file_type: found %d file(s)", len(parquet_files))

    # Map file_type → first matching file path
    type_to_file: dict[str, str] = {}
    for path in parquet_files:
        filename = _basename(path)

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
        cols = _get_schema_columns(path)
        if cols:
            result[file_type] = cols
            logger.debug(
                "get_columns_by_file_type: file_type='%s' → %d column(s)",
                file_type, len(cols),
            )
        else:
            logger.error(
                "get_columns_by_file_type: failed to read schema for file_type='%s' path='%s'",
                file_type, path,
            )
            result[file_type] = []

    logger.info(
        "get_columns_by_file_type: done — %d file type(s) discovered in '%s'",
        len(result), farm_dir,
    )
    return result


def _files_for_type(farm_dir: str, file_type: str) -> list[str]:
    """Return all parquet file paths/URLs that belong to the given file_type."""
    if _is_s3_path(farm_dir):
        # farm_dir is an S3 prefix like s3://bucket/kelmarsh/
        # List all files in that prefix and filter by convention
        all_files = _list_farm_parquet_files(farm_dir)
        matched = []
        # Convention 1: data_turbine_N.parquet / status_turbine_N.parquet
        pattern1 = re.compile(rf"^{re.escape(file_type)}_turbine_\d+\.parquet$")
        # Convention 2: T{NN}_{file_type}.parquet
        pattern2 = re.compile(rf"^T\d+_{re.escape(file_type)}\.parquet$")
        for path in all_files:
            fname = _basename(path)
            if pattern1.match(fname) or pattern2.match(fname):
                matched.append(path)
        matched.sort()
        logger.debug(
            "_files_for_type: file_type='%s' → %d file(s) matched in '%s'",
            file_type, len(matched), farm_dir,
        )
        return matched

    pattern_scada = os.path.join(farm_dir, f"{file_type}_turbine_*.parquet")
    pattern_hot = os.path.join(farm_dir, f"T*_{file_type}.parquet")
    files = sorted(glob.glob(pattern_scada) + glob.glob(pattern_hot))
    logger.debug(
        "_files_for_type: file_type='%s' → %d file(s) matched in '%s'",
        file_type, len(files), farm_dir,
    )
    return files


def count_turbines_in_files(files: list[str]) -> int:
    """Count unique turbine indices in a list of file paths/URLs."""
    turbine_ids: set[str] = set()
    for path in files:
        filename = _basename(path)
        m = re.match(r"^(?:data|status)_turbine_(\d+)\.parquet$", filename)
        if m:
            turbine_ids.add(m.group(1))
            continue
        m = re.match(r"^T(\d+)_", filename)
        if m:
            turbine_ids.add(m.group(1))
    return len(turbine_ids)


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
    file_list = ", ".join(f"'{p}'" for p in files)

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

    conn = _make_duckdb_conn()
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

