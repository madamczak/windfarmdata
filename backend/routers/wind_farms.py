"""
Wind farms router — endpoints related to wind farm listing and metadata.
"""

import logging
import os
import glob
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
)

logger = logging.getLogger("windfarm.routers.wind_farms")

router = APIRouter(prefix="/wind-farms", tags=["Wind Farms"])

# Canonical mapping: display name → directory name under data/
# Order determines the order returned by the API.
WIND_FARM_MAP: list[tuple[str, str]] = [
    ("Kelmarsh", "kelmarsh"),
    ("Penmanshiel", "penmanshiel"),
    ("Hill of Towie", "hill_of_towie"),
]


def count_turbines(farm_dir: str) -> int:
    """Count the number of unique turbines in a farm directory.

    Supports two file naming conventions:
      - Kelmarsh / Penmanshiel: data_turbine_1.parquet, status_turbine_1.parquet
        → counts unique N in *_turbine_N.parquet
      - Hill of Towie: T1_SomeSensor.parquet, T21_SCTurDigiOut.parquet
        → counts unique N in T{N}_*.parquet

    Returns the number of unique turbine indices found.
    """
    logger.debug("count_turbines: scanning '%s'", farm_dir)
    parquet_files = glob.glob(os.path.join(farm_dir, "*.parquet"))
    turbine_ids: set[str] = set()

    for path in parquet_files:
        filename = os.path.basename(path)

        # Convention 1: data_turbine_3.parquet / status_turbine_3.parquet
        m = re.match(r"^(?:data|status)_turbine_(\d+)\.parquet$", filename)
        if m:
            turbine_ids.add(m.group(1))
            continue

        # Convention 2: T21_SCTurDigiOut.parquet
        m = re.match(r"^T(\d+)_", filename)
        if m:
            turbine_ids.add(m.group(1))

    count = len(turbine_ids)
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
    base = os.path.abspath(settings.parquet_base_path)
    logger.info("list_wind_farms: base path='%s'", base)

    available: list[WindFarm] = []
    for display_name, dir_name in WIND_FARM_MAP:
        farm_path = os.path.join(base, dir_name)
        if os.path.isdir(farm_path):
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
    base = os.path.abspath(settings.parquet_base_path)
    logger.info("get_wind_farm_time_ranges: base path='%s'", base)

    results = []
    for _, dir_name in WIND_FARM_MAP:
        farm_dir = os.path.join(base, dir_name)
        if not os.path.isdir(farm_dir):
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
SCADA_FARMS = ["kelmarsh", "penmanshiel", "hill_of_towie"]


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
    base = os.path.abspath(settings.parquet_base_path)
    logger.info("get_wind_farm_columns: base path='%s'", base)

    farms = []
    for dir_name in SCADA_FARMS:
        farm_dir = os.path.join(base, dir_name)
        if not os.path.isdir(farm_dir):
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

    base = os.path.abspath(settings.parquet_base_path)

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

    farm_dir = os.path.join(base, farm)
    if not os.path.isdir(farm_dir):
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
