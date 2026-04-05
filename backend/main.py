"""
Wind Farm Data API — entry point.

Run locally with:
    uvicorn backend.main:app --reload
"""

import logging
import os
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
# CORS origins — configurable via CORS_ORIGINS env var (comma-separated).
# Defaults cover local Vite dev server and the Docker Nginx container.
# In production set CORS_ORIGINS to your real frontend domain.
# ---------------------------------------------------------------------------
_default_origins = [
    "http://localhost:5173",   # Vite dev server
    "http://127.0.0.1:5173",   # Vite dev server (IP)
    "http://localhost:80",     # Docker Nginx
    "http://localhost",        # Docker Nginx (no explicit port)
]
_env_origins = os.environ.get("CORS_ORIGINS", "")
CORS_ORIGINS = (
    [o.strip() for o in _env_origins.split(",") if o.strip()]
    if _env_origins
    else _default_origins
)

# ---------------------------------------------------------------------------
# Lifespan — startup / shutdown logging
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Wind Farm Data API starting up — version %s", app.version)
    logger.debug("CORS origins: %s", CORS_ORIGINS)
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
# CORS — origins controlled by CORS_ORIGINS (see top of file)
# ---------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
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

