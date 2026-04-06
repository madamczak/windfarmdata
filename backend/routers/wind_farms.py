"""
Wind farms router — endpoints related to wind farm listing and metadata.
"""

import logging
import os
import re
from datetime import date
from typing import Annotated
from fastapi import APIRouter, HTTPException, Query
from backend.models.schemas import (
    WindFarm,
    WindFarmsResponse,
    TimeRange,
    WindFarmTimeRangesResponse,
    FarmColumns,
    FarmColumnsResponse,
    DayDataResponse,
)
from backend.config import settings
from backend.services.query_service import (
    get_time_range,
    get_columns_by_file_type,
    get_data_for_date,
    count_turbines_in_files,
    _list_farm_parquet_files,
    _is_s3_path,
)

logger = logging.getLogger("windfarm.routers.wind_farms")

router = APIRouter(prefix="/wind-farms", tags=["Wind Farms"])

# Canonical mapping: display name → directory name.
# Hill of Towie is excluded — only Kelmarsh and Penmanshiel are active.
WIND_FARM_MAP: list[tuple[str, str]] = [
    ("Kelmarsh", "kelmarsh"),
    ("Penmanshiel", "penmanshiel"),
]


def _resolve_farm_dir(dir_name: str) -> str:
    """Return the path or S3 prefix for *dir_name*.

    In local mode:  data/<dir_name>  (local filesystem path)
    In r2 mode:     s3://<bucket>/<dir_name>/  (S3 prefix URL — no download)
    """
    if settings.storage_backend == "r2":
        from backend.services.r2_service import get_farm_prefix
        return get_farm_prefix(dir_name)
    base = os.path.abspath(settings.parquet_base_path)
    return os.path.join(base, dir_name)


def _farm_exists(farm_dir: str) -> bool:
    """Check whether a farm directory / S3 prefix actually contains parquet files."""
    if _is_s3_path(farm_dir):
        # For R2, existence means there is at least one parquet file in the prefix
        files = _list_farm_parquet_files(farm_dir)
        return len(files) > 0
    return os.path.isdir(farm_dir)


def count_turbines(farm_dir: str) -> int:
    """Count the number of unique turbines in a farm directory or S3 prefix.

    Delegates to count_turbines_in_files from query_service which handles
    both local paths and S3 URLs transparently.
    """
    logger.debug("count_turbines: scanning '%s'", farm_dir)
    all_files = _list_farm_parquet_files(farm_dir)
    count = count_turbines_in_files(all_files)
    logger.debug("count_turbines: found %d turbine(s) in '%s'", count, farm_dir)
    return count


@router.get(
    "",
    response_model=WindFarmsResponse,
    summary="List all wind farms",
    description=(
        "Returns the names of all available wind farms, their corresponding "
        "data directory names, and the number of turbines in each farm. "
        "Only farms whose directory exists under the configured parquet base "
        "path are included."
    ),
)
def list_wind_farms() -> WindFarmsResponse:
    """Return wind farms whose data directory exists on disk, with turbine counts."""
    logger.info("list_wind_farms: storage_backend='%s'", settings.storage_backend)

    available: list[WindFarm] = []
    for display_name, dir_name in WIND_FARM_MAP:
        farm_path = _resolve_farm_dir(dir_name)
        if _farm_exists(farm_path):
            turbine_count = count_turbines(farm_path)
            logger.debug(
                "list_wind_farms: farm='%s' dir='%s' turbines=%d",
                display_name, dir_name, turbine_count,
            )
            available.append(
                WindFarm(
                    name=display_name,
                    directory=dir_name,
                    turbine_count=turbine_count,
                )
            )
        else:
            logger.warning(
                "list_wind_farms: directory not found for farm='%s' path='%s'",
                display_name, farm_path,
            )

    logger.info("list_wind_farms: returning %d farm(s)", len(available))
    return WindFarmsResponse(wind_farms=available, total=len(available))


@router.get(
    "/time-ranges",
    response_model=WindFarmTimeRangesResponse,
    summary="Get time ranges for all wind farms",
    description=(
        "Returns the earliest and latest timestamps found across all parquet "
        "files for each wind farm. Useful for understanding the time span of "
        "each dataset before querying. Uses DuckDB for efficient min/max "
        "scanning without loading full datasets into memory."
    ),
)
def get_wind_farm_time_ranges() -> WindFarmTimeRangesResponse:
    """Scan all parquet files per farm and return their earliest/latest timestamps."""
    logger.info("get_wind_farm_time_ranges: storage_backend='%s'", settings.storage_backend)

    results = []
    for _, dir_name in WIND_FARM_MAP:
        farm_dir = _resolve_farm_dir(dir_name)
        if not _farm_exists(farm_dir):
            logger.warning(
                "get_wind_farm_time_ranges: skipping '%s' — directory not found",
                dir_name,
            )
            continue

        logger.debug("get_wind_farm_time_ranges: scanning farm='%s'", dir_name)
        earliest, latest, ts_col = get_time_range(farm_dir)

        if earliest is None:
            logger.warning(
                "get_wind_farm_time_ranges: farm='%s' — no timestamp data found",
                dir_name,
            )
        else:
            logger.info(
                "get_wind_farm_time_ranges: farm='%s' ts_col='%s' earliest=%s latest=%s",
                dir_name, ts_col, earliest, latest,
            )

        results.append(
            TimeRange(
                farm=dir_name,
                earliest=earliest,
                latest=latest,
                timestamp_column=ts_col,
            )
        )

    logger.info("get_wind_farm_time_ranges: returning %d result(s)", len(results))
    return WindFarmTimeRangesResponse(time_ranges=results)


# Farms supported by the /columns endpoint.
SCADA_FARMS = ["kelmarsh", "penmanshiel"]


@router.get(
    "/columns",
    response_model=FarmColumnsResponse,
    summary="Get column names for all wind farms",
    description=(
        "Returns the full set of column names grouped by file type for "
        "Kelmarsh, Penmanshiel, and Hill of Towie. "
        "For Kelmarsh and Penmanshiel the file types are 'data' and 'status'. "
        "For Hill of Towie the file types are the sensor table names "
        "(e.g. SCTurbine, AlarmLog, SCTurGrid, etc.). "
        "Column names are read from parquet metadata only — no row data is "
        "loaded."
    ),
)
def get_wind_farm_columns() -> FarmColumnsResponse:
    """Read parquet schemas and return column names grouped by file type."""
    logger.info("get_wind_farm_columns: storage_backend='%s'", settings.storage_backend)

    farms = []
    for dir_name in SCADA_FARMS:
        farm_dir = _resolve_farm_dir(dir_name)
        if not _farm_exists(farm_dir):
            logger.warning(
                "get_wind_farm_columns: skipping '%s' — directory not found",
                dir_name,
            )
            continue

        logger.debug("get_wind_farm_columns: reading schemas for farm='%s'", dir_name)
        columns_by_type = get_columns_by_file_type(farm_dir)

        for ftype, cols in columns_by_type.items():
            logger.debug(
                "get_wind_farm_columns: farm='%s' file_type='%s' columns=%d",
                dir_name, ftype, len(cols),
            )

        farms.append(
            FarmColumns(
                farm=dir_name,
                columns_by_type=columns_by_type,
            )
        )

    logger.info("get_wind_farm_columns: returning schema for %d farm(s)", len(farms))
    return FarmColumnsResponse(farms=farms)


# ---------------------------------------------------------------------------
# Known valid farm directory names — used for path validation
# ---------------------------------------------------------------------------
VALID_FARMS = {dir_name for _, dir_name in WIND_FARM_MAP}

# Known valid file types per farm.
# These must match the naming conventions used by the parquet files on disk.
# Kelmarsh / Penmanshiel: <file_type>_turbine_N.parquet
# Hill of Towie:          T{N}_<file_type>.parquet
VALID_FILE_TYPES: dict[str, set[str]] = {
    "kelmarsh":   {"data", "status"},
    "penmanshiel": {"data", "status"},
}


@router.get(
    "/{farm}/data/{query_date}",
    response_model=DayDataResponse,
    summary="Get data for a specific day",
    description=(
        "Returns all rows for the given farm, file type, and calendar date. "
        "Optionally restrict the response to a subset of columns via the "
        "`columns` query parameter (repeat the parameter for each column, "
        "e.g. `?columns=Wind+speed+(m/s)&columns=Power+(kW)`). "
        "When no columns are supplied every column in the file is returned. "
        "The timestamp column is always included. "
        "Use `file_type` to choose between file groups — for Kelmarsh and "
        "Penmanshiel use `data` or `status`; for Hill of Towie use a sensor "
        "table name such as `SCTurbine`, `AlarmLog`, `SCTurGrid`, etc."
    ),
)
def get_day_data(
    farm: str,
    query_date: date,
    file_type: Annotated[
        str,
        Query(description="File-type group to query, e.g. 'data', 'status', 'SCTurbine'"),
    ] = "data",
    columns: Annotated[
        list[str] | None,
        Query(description="Column names to include. Omit to return all columns."),
    ] = None,
) -> DayDataResponse:
    """Query parquet files for a single calendar day with optional column selection."""
    logger.info(
        "get_day_data: farm='%s' date=%s file_type='%s' columns_requested=%s",
        farm, query_date, file_type,
        columns if columns else "(all)",
    )


    # Validate farm name to prevent path traversal
    if farm not in VALID_FARMS:
        logger.warning(
            "get_day_data: rejected unknown farm='%s' (valid: %s)",
            farm, sorted(VALID_FARMS),
        )
        raise HTTPException(
            status_code=404,
            detail=f"Farm '{farm}' not found. Valid farms: {sorted(VALID_FARMS)}",
        )

    # Validate file_type before hitting the filesystem — must be a known type for this farm
    valid_types = VALID_FILE_TYPES.get(farm, set())
    if file_type not in valid_types:
        logger.warning(
            "get_day_data: rejected unknown file_type='%s' for farm='%s' (valid: %s)",
            file_type, farm, sorted(valid_types),
        )
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unknown file_type '{file_type}' for farm '{farm}'. "
                f"Valid types: {sorted(valid_types)}"
            ),
        )

    farm_dir = _resolve_farm_dir(farm)
    if not _farm_exists(farm_dir):
        logger.error(
            "get_day_data: farm='%s' — directory missing at '%s'",
            farm, farm_dir,
        )
        raise HTTPException(status_code=404, detail=f"Data directory for '{farm}' not found.")

    logger.debug(
        "get_day_data: querying farm_dir='%s' file_type='%s' date=%s",
        farm_dir, file_type, query_date,
    )

    try:
        col_names, rows = get_data_for_date(
            farm_dir=farm_dir,
            file_type=file_type,
            query_date=query_date,
            columns=columns or None,
        )
    except ValueError as exc:
        logger.warning(
            "get_day_data: bad request for farm='%s' file_type='%s' date=%s — %s",
            farm, file_type, query_date, exc,
        )
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    logger.info(
        "get_day_data: farm='%s' file_type='%s' date=%s — returned %d row(s) across %d column(s)",
        farm, file_type, query_date, len(rows), len(col_names),
    )

    return DayDataResponse(
        farm=farm,
        file_type=file_type,
        date=query_date.isoformat(),
        columns=col_names,
        row_count=len(rows),
        rows=rows,
    )
