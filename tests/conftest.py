"""
Shared pytest fixtures and configuration for the wind farm API test suite.
"""

import os
from datetime import datetime
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from backend.main import app


# ---------------------------------------------------------------------------
# FastAPI test client
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def client() -> TestClient:
    """Return a TestClient wired to the FastAPI application."""
    return TestClient(app)


# ---------------------------------------------------------------------------
# Temporary parquet data fixtures
# ---------------------------------------------------------------------------

def _write_parquet(path: str, table: pa.Table) -> None:
    """Write a PyArrow table to *path* as a parquet file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(table, path)


@pytest.fixture(scope="session")
def tmp_farm_dir(tmp_path_factory):
    """
    Create a small, self-contained fake farm directory tree under a
    temporary folder so that query_service functions can be exercised
    without touching the real data/ directory.

    Layout produced:
        <tmp>/
            kelmarsh_mock/
                data_turbine_1.parquet      (timestamp: "Date and time")
                status_turbine_1.parquet    (timestamp: "Timestamp start")
            hill_of_towie_mock/
                T01_SCTurbine.parquet       (timestamp: "TimeStamp")
    """
    base = tmp_path_factory.mktemp("farm_data")

    # --- Kelmarsh-style data file ---
    kelmarsh_dir = base / "kelmarsh_mock"
    data = {
        "Date and time": pa.array(
            [
                datetime(2021, 6, 1, 0, 10, 0),
                datetime(2021, 6, 1, 0, 20, 0),
                datetime(2021, 6, 2, 0, 10, 0),
            ],
            type=pa.timestamp("s"),
        ),
        "Wind speed (m/s)": pa.array([5.1, 6.2, 7.3], type=pa.float64()),
        "Power (kW)": pa.array([100.0, 200.0, 300.0], type=pa.float64()),
    }
    _write_parquet(
        str(kelmarsh_dir / "data_turbine_1.parquet"),
        pa.table(data),
    )

    # --- Kelmarsh-style status file ---
    status = {
        "Timestamp start": pa.array(
            [
                datetime(2021, 6, 1, 1, 0, 0),
                datetime(2021, 6, 2, 2, 0, 0),
            ],
            type=pa.timestamp("s"),
        ),
        "Status": pa.array(["OK", "WARNING"], type=pa.string()),
        "Duration (s)": pa.array([3600, 7200], type=pa.int64()),
    }
    _write_parquet(
        str(kelmarsh_dir / "status_turbine_1.parquet"),
        pa.table(status),
    )

    # --- Hill-of-Towie-style file ---
    hot_dir = base / "hill_of_towie_mock"
    hot_data = {
        "TimeStamp": pa.array(
            [
                datetime(2016, 7, 19, 0, 10, 0),
                datetime(2016, 7, 19, 0, 20, 0),
            ],
            type=pa.timestamp("s"),
        ),
        "ActivePower": pa.array([50.0, 60.0], type=pa.float64()),
    }
    _write_parquet(
        str(hot_dir / "T01_SCTurbine.parquet"),
        pa.table(hot_data),
    )

    return base

