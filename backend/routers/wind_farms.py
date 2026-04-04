"""
Wind farms router — endpoints related to wind farm listing and metadata.
"""

import os
import glob
import re
from fastapi import APIRouter
from backend.models.schemas import (
    WindFarm,
    WindFarmsResponse,
    TimeRange,
    WindFarmTimeRangesResponse,
    FarmColumns,
    FarmColumnsResponse,
)
from backend.config import settings
from backend.services.query_service import get_time_range, get_columns_by_file_type

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

    return len(turbine_ids)


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

    available = [
        WindFarm(
            name=display_name,
            directory=dir_name,
            turbine_count=count_turbines(os.path.join(base, dir_name)),
        )
        for display_name, dir_name in WIND_FARM_MAP
        if os.path.isdir(os.path.join(base, dir_name))
    ]

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

    results = []
    for _, dir_name in WIND_FARM_MAP:
        farm_dir = os.path.join(base, dir_name)
        if not os.path.isdir(farm_dir):
            continue

        earliest, latest, ts_col = get_time_range(farm_dir)
        results.append(
            TimeRange(
                farm=dir_name,
                earliest=earliest,
                latest=latest,
                timestamp_column=ts_col,
            )
        )

    return WindFarmTimeRangesResponse(time_ranges=results)


# Farms supported by the /columns endpoint.
# Kelmarsh/Penmanshiel use data_turbine_N / status_turbine_N naming.
# Hill of Towie uses T{NN}_{SensorType} naming — both are handled by
# get_columns_by_file_type().
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

    farms = []
    for dir_name in SCADA_FARMS:
        farm_dir = os.path.join(base, dir_name)
        if not os.path.isdir(farm_dir):
            continue

        columns_by_type = get_columns_by_file_type(farm_dir)
        farms.append(
            FarmColumns(
                farm=dir_name,
                columns_by_type=columns_by_type,
            )
        )

    return FarmColumnsResponse(farms=farms)


