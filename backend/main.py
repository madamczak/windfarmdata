"""
Wind Farm Data API — entry point.

Run locally with:
    uvicorn backend.main:app --reload
"""

from fastapi import FastAPI
from backend.routers import wind_farms

app = FastAPI(
    title="Wind Farm Data API",
    description=(
        "REST API for querying time-series sensor and status data "
        "from Kelmarsh, Penmanshiel, and Hill of Towie wind farms."
    ),
    version="0.1.0",
)

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------
app.include_router(wind_farms.router)

