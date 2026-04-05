"""
Wind Farm Data API — entry point.

Run locally with:
    uvicorn backend.main:app --reload
"""

import logging
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
import uvicorn
from backend.routers import wind_farms

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("windfarm.main")

# ---------------------------------------------------------------------------
# Lifespan — startup / shutdown logging
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Wind Farm Data API starting up — version %s", app.version)
    logger.debug("CORS origins: %s", ["http://localhost:5173", "http://127.0.0.1:5173"])
    yield
    logger.info("Wind Farm Data API shutting down.")


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Wind Farm Data API",
    description=(
        "REST API for querying time-series sensor and status data "
        "from Kelmarsh, Penmanshiel, and Hill of Towie wind farms."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# CORS — allow the Vue dev server (port 5173) to call the API
# ---------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Request / response logging middleware
# ---------------------------------------------------------------------------

@app.middleware("http")
async def log_requests(request: Request, call_next) -> Response:
    """Log every incoming request and its response status + duration."""
    start = time.perf_counter()
    logger.info(
        "REQUEST  %s %s  client=%s  query=%s",
        request.method,
        request.url.path,
        request.client.host if request.client else "unknown",
        str(request.query_params) or "(none)",
    )
    response: Response = await call_next(request)
    elapsed_ms = (time.perf_counter() - start) * 1000
    logger.info(
        "RESPONSE %s %s  status=%d  duration=%.1f ms",
        request.method,
        request.url.path,
        response.status_code,
        elapsed_ms,
    )
    return response


# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------
app.include_router(wind_farms.router)
logger.debug("Router registered: %s", wind_farms.router.prefix)


if __name__ == "__main__":
    uvicorn.run("backend.main:app", host="127.0.0.1", port=8000, reload=True)

