"""
Wind farms router — endpoints related to wind farm listing and metadata.
"""

import os
import glob
import re
from fastapi import APIRouter
from backend.models.schemas import WindFarm, WindFarmsResponse
from backend.config import settings

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

