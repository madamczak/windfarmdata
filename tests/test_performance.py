"""
Performance benchmarks for the Wind Farm Data API service layer.

Uses pytest-benchmark to time the core query functions against the mock
parquet data created by the tmp_farm_dir fixture in conftest.py.

Run benchmarks locally:
    pytest tests/test_performance.py -v --benchmark-only

Compare against a saved baseline:
    pytest tests/test_performance.py --benchmark-compare=.benchmarks/baseline.json

Save a new baseline:
    pytest tests/test_performance.py --benchmark-save=baseline

The CI workflow:
  1. Runs benchmarks and saves results as a JSON artefact.
  2. On subsequent runs, downloads the previous artefact and compares.
  3. A custom comparison script (scripts/check_benchmark_regression.py) fails
     the build if any benchmark is more than 5 % slower than the baseline.
"""

import pytest
from datetime import date

from backend.services.query_service import (
    get_time_range,
    get_columns_by_file_type,
    get_data_for_date,
    _detect_timestamp_column_from_schema,
    _files_for_type,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _kelmarsh(tmp_farm_dir):
    return str(tmp_farm_dir / "kelmarsh")


def _hot(tmp_farm_dir):
    return str(tmp_farm_dir / "hill_of_towie")


# ---------------------------------------------------------------------------
# Schema / metadata benchmarks (should be microseconds — pure I/O metadata)
# ---------------------------------------------------------------------------

class TestSchemaBenchmarks:
    """Benchmark metadata-only operations (no data is read into memory)."""

    def test_detect_timestamp_column(self, benchmark, tmp_farm_dir):
        """Time taken to detect the timestamp column from parquet schema metadata."""
        parquet_file = str(tmp_farm_dir / "kelmarsh" / "data_turbine_1.parquet")
        result = benchmark(
            _detect_timestamp_column_from_schema,
            parquet_file,
        )
        # Sanity check — must still return the correct column
        assert result == "Date and time"

    def test_get_columns_by_file_type_kelmarsh(self, benchmark, tmp_farm_dir):
        """Time taken to discover and read all column schemas for Kelmarsh."""
        result = benchmark(get_columns_by_file_type, _kelmarsh(tmp_farm_dir))
        assert "data" in result
        assert "status" in result

    def test_get_columns_by_file_type_hill_of_towie(self, benchmark, tmp_farm_dir):
        """Time taken to discover and read all column schemas for Hill of Towie."""
        result = benchmark(get_columns_by_file_type, _hot(tmp_farm_dir))
        assert "SCTurbine" in result

    def test_files_for_type(self, benchmark, tmp_farm_dir):
        """Time taken to glob parquet files for a given file_type."""
        result = benchmark(_files_for_type, _kelmarsh(tmp_farm_dir), "data")
        assert len(result) == 1


# ---------------------------------------------------------------------------
# Time-range benchmarks (DuckDB MIN/MAX query — fast even on large files)
# ---------------------------------------------------------------------------

class TestTimeRangeBenchmarks:
    """Benchmark get_time_range() — scans all timestamp columns via DuckDB."""

    def test_time_range_kelmarsh(self, benchmark, tmp_farm_dir):
        """Time to compute earliest/latest timestamp across all Kelmarsh files."""
        earliest, latest, ts_col = benchmark(get_time_range, _kelmarsh(tmp_farm_dir))
        assert ts_col is not None
        assert earliest is not None
        assert latest is not None

    def test_time_range_hill_of_towie(self, benchmark, tmp_farm_dir):
        """Time to compute earliest/latest timestamp for Hill of Towie files."""
        earliest, latest, ts_col = benchmark(get_time_range, _hot(tmp_farm_dir))
        assert ts_col == "TimeStamp"
        assert earliest is not None


# ---------------------------------------------------------------------------
# Data query benchmarks (DuckDB full date-filter query)
# ---------------------------------------------------------------------------

class TestDataQueryBenchmarks:
    """Benchmark get_data_for_date() — the core data-retrieval hot path."""

    def test_query_all_columns_kelmarsh(self, benchmark, tmp_farm_dir):
        """Time to query all columns for one day — Kelmarsh data file type."""
        cols, rows = benchmark(
            get_data_for_date,
            farm_dir=_kelmarsh(tmp_farm_dir),
            file_type="data",
            query_date=date(2021, 6, 1),
            columns=None,
        )
        assert len(rows) == 2
        assert "Date and time" in cols

    def test_query_all_columns_status(self, benchmark, tmp_farm_dir):
        """Time to query all columns for one day — Kelmarsh status file type."""
        cols, rows = benchmark(
            get_data_for_date,
            farm_dir=_kelmarsh(tmp_farm_dir),
            file_type="status",
            query_date=date(2021, 6, 1),
            columns=None,
        )
        assert len(rows) == 1
        assert "Timestamp start" in cols

    def test_query_single_column_kelmarsh(self, benchmark, tmp_farm_dir):
        """Time to query a single projected column for one day."""
        cols, rows = benchmark(
            get_data_for_date,
            farm_dir=_kelmarsh(tmp_farm_dir),
            file_type="data",
            query_date=date(2021, 6, 1),
            columns=["Wind speed (m/s)"],
        )
        assert "Wind speed (m/s)" in cols
        assert "Power (kW)" not in cols

    def test_query_empty_date_kelmarsh(self, benchmark, tmp_farm_dir):
        """Time to query a date with no matching rows (filter-only, no data transfer)."""
        cols, rows = benchmark(
            get_data_for_date,
            farm_dir=_kelmarsh(tmp_farm_dir),
            file_type="data",
            query_date=date(1999, 1, 1),
            columns=None,
        )
        assert rows == []

    def test_query_hill_of_towie(self, benchmark, tmp_farm_dir):
        """Time to query Hill-of-Towie T{N}_ naming convention for one day."""
        cols, rows = benchmark(
            get_data_for_date,
            farm_dir=_hot(tmp_farm_dir),
            file_type="SCTurbine",
            query_date=date(2016, 7, 19),
            columns=None,
        )
        assert len(rows) == 2
        assert "TimeStamp" in cols

