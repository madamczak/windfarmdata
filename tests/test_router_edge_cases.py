"""
Tests for edge-case branches in backend/routers/wind_farms.py and
backend/services/query_service.py that are not covered by the main test files.

Targets the following uncovered lines (as reported by coverage):
  wind_farms.py  : 108, 137-141, 147, 197-201, 308-312, 326-331
  query_service.py: 50-59, 80-84, 121-126, 184-189, 238-241, 283-288
"""

import os
import glob
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock

from backend.main import app
from backend import config as backend_config
from backend.services import query_service
from backend.services.query_service import (
    get_time_range,
    get_columns_by_file_type,
    get_data_for_date,
    _detect_timestamp_column_from_schema,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_parquet(path: str, table: pa.Table) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(table, path)


# ---------------------------------------------------------------------------
# _detect_timestamp_column_from_schema — exception path (lines 50-59)
# ---------------------------------------------------------------------------

class TestDetectTimestampColumnFromSchema:
    """Tests for the private helper _detect_timestamp_column_from_schema."""

    def test_returns_none_for_nonexistent_file(self, tmp_path):
        """A file that does not exist should return None, not raise."""
        result = _detect_timestamp_column_from_schema(str(tmp_path / "missing.parquet"))
        assert result is None

    def test_returns_none_for_corrupt_file(self, tmp_path):
        """A corrupt (non-parquet) file should return None, not raise."""
        bad = tmp_path / "bad.parquet"
        bad.write_bytes(b"this is not a parquet file")
        result = _detect_timestamp_column_from_schema(str(bad))
        assert result is None

    def test_returns_none_when_no_matching_column(self, tmp_path):
        """A valid parquet file with no timestamp column should return None."""
        path = str(tmp_path / "no_ts.parquet")
        _write_parquet(path, pa.table({"value": pa.array([1.0, 2.0])}))
        result = _detect_timestamp_column_from_schema(path)
        assert result is None


# ---------------------------------------------------------------------------
# get_time_range — no-timestamp-groups branch (lines 80-84)
# ---------------------------------------------------------------------------

class TestGetTimeRangeNoBranch:
    """Tests for get_time_range when files exist but none have a timestamp col."""

    def test_returns_none_tuple_when_no_timestamp_cols(self, tmp_path):
        """Files with no recognised timestamp column must return (None, None, None)."""
        farm_dir = tmp_path / "farm"
        # Write a parquet file with no timestamp column
        _write_parquet(
            str(farm_dir / "data_turbine_1.parquet"),
            pa.table({"value": pa.array([1.0, 2.0])}),
        )
        result = get_time_range(str(farm_dir))
        assert result == (None, None, None)


# ---------------------------------------------------------------------------
# get_time_range — DuckDB exception branch (lines 121-126)
# ---------------------------------------------------------------------------

class TestGetTimeRangeDuckDBError:
    """get_time_range should handle DuckDB errors gracefully."""

    def test_continues_on_duckdb_error(self, tmp_farm_dir):
        """If DuckDB raises, get_time_range should return (None, None, None) gracefully."""
        kelmarsh_dir = str(tmp_farm_dir / "kelmarsh")
        with patch("backend.services.query_service.duckdb") as mock_duckdb:
            mock_conn = MagicMock()
            mock_conn.execute.side_effect = RuntimeError("DuckDB boom")
            mock_duckdb.connect.return_value = mock_conn
            result = get_time_range(kelmarsh_dir)
        # Should return (None, None, None) because all groups errored
        assert result == (None, None, None)


# ---------------------------------------------------------------------------
# get_columns_by_file_type — schema read exception (lines 184-189)
# ---------------------------------------------------------------------------

class TestGetColumnsByFileTypeError:
    """get_columns_by_file_type should survive a corrupt schema."""

    def test_returns_empty_list_for_bad_file(self, tmp_path):
        """A corrupt parquet file should produce an empty column list, not raise."""
        farm_dir = tmp_path / "farm"
        os.makedirs(str(farm_dir), exist_ok=True)
        bad = farm_dir / "data_turbine_1.parquet"
        bad.write_bytes(b"not a parquet file")
        result = get_columns_by_file_type(str(farm_dir))
        # The key should exist but the column list should be empty
        assert "data" in result
        assert result["data"] == []


# ---------------------------------------------------------------------------
# get_data_for_date — no files branch (lines 238-241)
# ---------------------------------------------------------------------------

class TestGetDataForDateNoFiles:
    """get_data_for_date should raise ValueError when no files match."""

    def test_raises_value_error_when_no_files(self, tmp_path):
        """Asking for a file_type that has no files must raise ValueError."""
        farm_dir = tmp_path / "farm"
        os.makedirs(str(farm_dir), exist_ok=True)
        with pytest.raises(ValueError, match="No parquet files found"):
            get_data_for_date(
                farm_dir=str(farm_dir),
                file_type="nonexistent",
                query_date=date(2021, 6, 1),
            )


# ---------------------------------------------------------------------------
# get_data_for_date — DuckDB exception branch (lines 283-288)
# ---------------------------------------------------------------------------

class TestGetDataForDateDuckDBError:
    """get_data_for_date should propagate DuckDB exceptions."""

    def test_raises_on_duckdb_error(self, tmp_farm_dir):
        """A DuckDB runtime error must propagate (not be silently swallowed)."""
        kelmarsh_dir = str(tmp_farm_dir / "kelmarsh")
        with patch("backend.services.query_service.duckdb") as mock_duckdb:
            mock_conn = MagicMock()
            mock_conn.execute.side_effect = RuntimeError("DuckDB query failed")
            mock_duckdb.connect.return_value = mock_conn
            with pytest.raises(RuntimeError, match="DuckDB query failed"):
                get_data_for_date(
                    farm_dir=kelmarsh_dir,
                    file_type="data",
                    query_date=date(2021, 6, 1),
                )


# ---------------------------------------------------------------------------
# Router — list_wind_farms with missing farm directory (line 108)
# ---------------------------------------------------------------------------

class TestListWindFarmsMissingDir:
    """list_wind_farms should skip farms whose directory does not exist."""

    def test_missing_farm_dir_is_skipped(self, tmp_path):
        """If a farm directory is absent the farm is not included in the response."""
        # Only create kelmarsh, not penmanshiel or hill_of_towie
        kelmarsh_dir = tmp_path / "kelmarsh"
        _write_parquet(
            str(kelmarsh_dir / "data_turbine_1.parquet"),
            pa.table({
                "Date and time": pa.array(
                    [__import__("datetime").datetime(2021, 6, 1)],
                    type=pa.timestamp("s"),
                ),
                "Power (kW)": pa.array([100.0]),
            }),
        )
        original = backend_config.settings.parquet_base_path
        backend_config.settings.parquet_base_path = str(tmp_path)
        try:
            client = TestClient(app)
            data = client.get("/wind-farms").json()
        finally:
            backend_config.settings.parquet_base_path = original

        dirs = {f["directory"] for f in data["wind_farms"]}
        assert "kelmarsh" in dirs
        assert "penmanshiel" not in dirs
        assert "hill_of_towie" not in dirs


# ---------------------------------------------------------------------------
# Router — time-ranges with missing farm directory (lines 137-141)
# and farm with no timestamp data (line 147)
# ---------------------------------------------------------------------------

class TestTimeRangesMissingOrEmpty:
    """time-ranges endpoint edge cases."""

    def test_missing_farm_dir_skipped_in_time_ranges(self, tmp_path):
        """Farms with no directory on disk are omitted from time-ranges."""
        # Only kelmarsh exists
        kelmarsh_dir = tmp_path / "kelmarsh"
        _write_parquet(
            str(kelmarsh_dir / "data_turbine_1.parquet"),
            pa.table({
                "Date and time": pa.array(
                    [__import__("datetime").datetime(2021, 6, 1)],
                    type=pa.timestamp("s"),
                ),
                "Power (kW)": pa.array([100.0]),
            }),
        )
        original = backend_config.settings.parquet_base_path
        backend_config.settings.parquet_base_path = str(tmp_path)
        try:
            client = TestClient(app)
            data = client.get("/wind-farms/time-ranges").json()
        finally:
            backend_config.settings.parquet_base_path = original

        farms_returned = {e["farm"] for e in data["time_ranges"]}
        assert "kelmarsh" in farms_returned
        assert "penmanshiel" not in farms_returned

    def test_farm_with_no_timestamp_returns_null_range(self, tmp_path):
        """A farm directory with files but no timestamp column returns null values."""
        farm_dir = tmp_path / "kelmarsh"
        _write_parquet(
            str(farm_dir / "data_turbine_1.parquet"),
            pa.table({"value": pa.array([1.0, 2.0])}),
        )
        original = backend_config.settings.parquet_base_path
        backend_config.settings.parquet_base_path = str(tmp_path)
        try:
            client = TestClient(app)
            data = client.get("/wind-farms/time-ranges").json()
        finally:
            backend_config.settings.parquet_base_path = original

        entry = next(e for e in data["time_ranges"] if e["farm"] == "kelmarsh")
        assert entry["earliest"] is None
        assert entry["latest"] is None
        assert entry["timestamp_column"] is None


# ---------------------------------------------------------------------------
# Router — columns with missing farm directory (lines 197-201)
# ---------------------------------------------------------------------------

class TestColumnsMissingDir:
    """columns endpoint should skip farms without a directory."""

    def test_missing_farm_dir_skipped_in_columns(self, tmp_path):
        """Farms with no directory are not present in /columns response."""
        kelmarsh_dir = tmp_path / "kelmarsh"
        _write_parquet(
            str(kelmarsh_dir / "data_turbine_1.parquet"),
            pa.table({
                "Date and time": pa.array(
                    [__import__("datetime").datetime(2021, 6, 1)],
                    type=pa.timestamp("s"),
                ),
                "Power (kW)": pa.array([100.0]),
            }),
        )
        original = backend_config.settings.parquet_base_path
        backend_config.settings.parquet_base_path = str(tmp_path)
        try:
            client = TestClient(app)
            data = client.get("/wind-farms/columns").json()
        finally:
            backend_config.settings.parquet_base_path = original

        farms_returned = {e["farm"] for e in data["farms"]}
        assert "kelmarsh" in farms_returned
        assert "penmanshiel" not in farms_returned


# ---------------------------------------------------------------------------
# Router — get_day_data farm dir missing after validation (lines 308-312)
# ---------------------------------------------------------------------------

class TestDayDataFarmDirMissing:
    """get_day_data should 404 when the farm directory is absent after name validation."""

    def test_missing_farm_dir_returns_404(self, tmp_path):
        """Valid farm name but missing directory on disk must return HTTP 404."""
        # parquet_base_path points to an empty tmp dir — 'kelmarsh' subdir doesn't exist
        original = backend_config.settings.parquet_base_path
        backend_config.settings.parquet_base_path = str(tmp_path)
        try:
            client = TestClient(app)
            response = client.get(
                "/wind-farms/kelmarsh/data/2021-06-01",
                params={"file_type": "data"},
            )
        finally:
            backend_config.settings.parquet_base_path = original

        assert response.status_code == 404


# ---------------------------------------------------------------------------
# Router — get_day_data ValueError from query_service (lines 326-331)
# ---------------------------------------------------------------------------

class TestDayDataValueError:
    """get_day_data should return HTTP 400 when query_service raises ValueError."""

    def test_value_error_returns_400(self, mock_client):
        """A ValueError raised inside get_data_for_date must surface as HTTP 400."""
        with patch(
            "backend.routers.wind_farms.get_data_for_date",
            side_effect=ValueError("Cannot detect a timestamp column"),
        ):
            response = mock_client.get(
                "/wind-farms/kelmarsh/data/2021-06-01",
                params={"file_type": "data"},
            )
        assert response.status_code == 400
        assert "Cannot detect" in response.json()["detail"]


# ---------------------------------------------------------------------------
# main.py — lifespan startup/shutdown (lines 52-55)
# ---------------------------------------------------------------------------

class TestMainLifespan:
    """The FastAPI lifespan context (startup/shutdown) should execute without error."""

    def test_app_starts_and_stops_cleanly(self):
        """Using TestClient as context manager exercises the lifespan hooks."""
        with TestClient(app) as client:
            response = client.get("/wind-farms")
            assert response.status_code == 200

