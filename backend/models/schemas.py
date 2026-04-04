"""
Pydantic schemas for API request and response models.
"""

from datetime import datetime
from typing import Any
from pydantic import BaseModel


class WindFarm(BaseModel):
    """Represents a single wind farm entry."""

    name: str           # Human-readable display name, e.g. "Kelmarsh"
    directory: str      # Corresponding directory name under data/, e.g. "kelmarsh"
    turbine_count: int  # Number of turbines with data files in this farm's directory


class WindFarmsResponse(BaseModel):
    """Response model for the /wind-farms endpoint."""

    wind_farms: list[WindFarm]
    total: int


class TimeRange(BaseModel):
    """Earliest and latest timestamp found across all parquet files for a farm."""

    farm: str                        # Directory name, e.g. "kelmarsh"
    earliest: datetime | None        # Earliest timestamp across all turbines
    latest: datetime | None          # Latest timestamp across all turbines
    timestamp_column: str | None     # Name of the timestamp column detected


class WindFarmTimeRangesResponse(BaseModel):
    """Response model for the /wind-farms/time-ranges endpoint."""

    time_ranges: list[TimeRange]


class FarmColumns(BaseModel):
    """Column names for a single wind farm, grouped by file type."""

    farm: str                              # Directory name, e.g. "kelmarsh"
    columns_by_type: dict[str, list[str]]  # e.g. {"data": [...], "status": [...]}


class FarmColumnsResponse(BaseModel):
    """Response model for the /wind-farms/columns endpoint."""

    farms: list[FarmColumns]


class DayDataResponse(BaseModel):
    """Response model for the /wind-farms/{farm}/data/{date} endpoint."""

    farm: str                    # Directory name, e.g. "kelmarsh"
    file_type: str               # File-type group queried, e.g. "data" or "SCTurbine"
    date: str                    # ISO date string, e.g. "2021-03-15"
    columns: list[str]           # Column names in the order they appear in rows
    row_count: int               # Number of rows returned
    rows: list[list[Any]]        # Each inner list is one row of values


