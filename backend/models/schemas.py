"""
Pydantic schemas for API request and response models.
"""

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

