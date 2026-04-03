"""
Wind farms router — endpoints related to wind farm listing and metadata.
"""

import os
import glob
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
    """Count the number of turbines by counting data_turbine_*.parquet files.

    Falls back to counting status_turbine_*.parquet files if no data files exist.
    Each file represents one turbine.
    """
    data_files = glob.glob(os.path.join(farm_dir, "data_turbine_*.parquet"))
    if data_files:
        return len(data_files)
    # Fallback: count status files
    status_files = glob.glob(os.path.join(farm_dir, "status_turbine_*.parquet"))
    return len(status_files)


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

