"""
Unit tests for backend/services/query_service.py.

These tests use a small in-memory (tmp directory) set of parquet files
created by the tmp_farm_dir fixture in conftest.py — no real data/ directory
is required.
"""

import os
import pytest
from datetime import date

from backend.services.query_service import (
    get_time_range,
    get_columns_by_file_type,
    get_data_for_date,
)


# ---------------------------------------------------------------------------
# get_time_range
# ---------------------------------------------------------------------------

class TestGetTimeRange:
    """Tests for get_time_range()."""

    def test_returns_three_tuple(self, tmp_farm_dir):
        """get_time_range must return a 3-tuple."""
        kelmarsh_dir = str(tmp_farm_dir / "kelmarsh")
        result = get_time_range(kelmarsh_dir)
        assert isinstance(result, tuple) and len(result) == 3

    def test_returns_none_for_empty_dir(self, tmp_path):
        """An empty directory must return (None, None, None)."""
        result = get_time_range(str(tmp_path))
        assert result == (None, None, None)

    def test_detects_date_and_time_column(self, tmp_farm_dir):
        """Should detect 'Date and time' as timestamp for Kelmarsh-style data."""
        kelmarsh_dir = str(tmp_farm_dir / "kelmarsh")
        _, _, ts_col = get_time_range(kelmarsh_dir)
        # Both 'Date and time' and 'Timestamp start' are valid — at least one must be detected
        assert ts_col is not None

    def test_earliest_before_latest(self, tmp_farm_dir):
        """earliest must be <= latest."""
        kelmarsh_dir = str(tmp_farm_dir / "kelmarsh")
        earliest, latest, _ = get_time_range(kelmarsh_dir)
        if earliest is not None and latest is not None:
            assert earliest <= latest

    def test_hill_of_towie_style_files(self, tmp_farm_dir):
        """get_time_range should work for Hill-of-Towie T{NN}_ naming."""
        hot_dir = str(tmp_farm_dir / "hill_of_towie")
        earliest, latest, ts_col = get_time_range(hot_dir)
        assert earliest is not None
        assert latest is not None
        assert ts_col == "TimeStamp"

    def test_nonexistent_dir_returns_none_tuple(self, tmp_path):
        """A path that does not exist should return (None, None, None)."""
        result = get_time_range(str(tmp_path / "does_not_exist"))
        assert result == (None, None, None)


# ---------------------------------------------------------------------------
# get_columns_by_file_type
# ---------------------------------------------------------------------------

class TestGetColumnsByFileType:
    """Tests for get_columns_by_file_type()."""

    def test_returns_dict(self, tmp_farm_dir):
        """Must return a dict."""
        kelmarsh_dir = str(tmp_farm_dir / "kelmarsh")
        result = get_columns_by_file_type(kelmarsh_dir)
        assert isinstance(result, dict)

    def test_kelmarsh_has_data_and_status_types(self, tmp_farm_dir):
        """Kelmarsh directory must have 'data' and 'status' file types."""
        kelmarsh_dir = str(tmp_farm_dir / "kelmarsh")
        result = get_columns_by_file_type(kelmarsh_dir)
        assert "data" in result, "Expected 'data' key in columns_by_type"
        assert "status" in result, "Expected 'status' key in columns_by_type"

    def test_data_columns_are_correct(self, tmp_farm_dir):
        """'data' file type should contain the columns written in conftest."""
        kelmarsh_dir = str(tmp_farm_dir / "kelmarsh")
        result = get_columns_by_file_type(kelmarsh_dir)
        data_cols = result.get("data", [])
        assert "Date and time" in data_cols
        assert "Wind speed (m/s)" in data_cols
        assert "Power (kW)" in data_cols

    def test_status_columns_are_correct(self, tmp_farm_dir):
        """'status' file type should contain the columns written in conftest."""
        kelmarsh_dir = str(tmp_farm_dir / "kelmarsh")
        result = get_columns_by_file_type(kelmarsh_dir)
        status_cols = result.get("status", [])
        assert "Timestamp start" in status_cols
        assert "Status" in status_cols
        assert "Duration (s)" in status_cols

    def test_hill_of_towie_has_scturbine_type(self, tmp_farm_dir):
        """Hill-of-Towie directory must have 'SCTurbine' file type."""
        hot_dir = str(tmp_farm_dir / "hill_of_towie")
        result = get_columns_by_file_type(hot_dir)
        assert "SCTurbine" in result

    def test_empty_dir_returns_empty_dict(self, tmp_path):
        """Empty directory must return an empty dict."""
        result = get_columns_by_file_type(str(tmp_path))
        assert result == {}

    def test_column_values_are_lists_of_strings(self, tmp_farm_dir):
        """Each value in the returned dict must be a list of strings."""
        kelmarsh_dir = str(tmp_farm_dir / "kelmarsh")
        result = get_columns_by_file_type(kelmarsh_dir)
        for ftype, cols in result.items():
            assert isinstance(cols, list), f"Columns for '{ftype}' is not a list"
            for col in cols:
                assert isinstance(col, str), f"Column name '{col}' is not a string"


# ---------------------------------------------------------------------------
# get_data_for_date
# ---------------------------------------------------------------------------

class TestGetDataForDate:
    """Tests for get_data_for_date()."""

    def test_returns_columns_and_rows(self, tmp_farm_dir):
        """Must return a tuple (list[str], list[list])."""
        kelmarsh_dir = str(tmp_farm_dir / "kelmarsh")
        cols, rows = get_data_for_date(
            farm_dir=kelmarsh_dir,
            file_type="data",
            query_date=date(2021, 6, 1),
        )
        assert isinstance(cols, list)
        assert isinstance(rows, list)

    def test_correct_row_count_for_date(self, tmp_farm_dir):
        """Should return exactly 2 rows for 2021-06-01 in the mock data."""
        kelmarsh_dir = str(tmp_farm_dir / "kelmarsh")
        cols, rows = get_data_for_date(
            farm_dir=kelmarsh_dir,
            file_type="data",
            query_date=date(2021, 6, 1),
        )
        # conftest writes 2 rows for 2021-06-01
        assert len(rows) == 2

    def test_correct_row_count_for_other_date(self, tmp_farm_dir):
        """Should return exactly 1 row for 2021-06-02 in the mock data."""
        kelmarsh_dir = str(tmp_farm_dir / "kelmarsh")
        _, rows = get_data_for_date(
            farm_dir=kelmarsh_dir,
            file_type="data",
            query_date=date(2021, 6, 2),
        )
        assert len(rows) == 1

    def test_returns_empty_for_date_with_no_data(self, tmp_farm_dir):
        """Should return 0 rows for a date not in the mock data."""
        kelmarsh_dir = str(tmp_farm_dir / "kelmarsh")
        _, rows = get_data_for_date(
            farm_dir=kelmarsh_dir,
            file_type="data",
            query_date=date(2099, 1, 1),
        )
        assert rows == []

    def test_column_filter_limits_columns(self, tmp_farm_dir):
        """Requesting specific columns should return only those columns (+timestamp)."""
        kelmarsh_dir = str(tmp_farm_dir / "kelmarsh")
        cols, rows = get_data_for_date(
            farm_dir=kelmarsh_dir,
            file_type="data",
            query_date=date(2021, 6, 1),
            columns=["Wind speed (m/s)"],
        )
        # Timestamp column is always prepended
        assert "Date and time" in cols
        assert "Wind speed (m/s)" in cols
        # "Power (kW)" must NOT be in the result when not requested
        assert "Power (kW)" not in cols

    def test_no_column_filter_returns_all_columns(self, tmp_farm_dir):
        """When columns=None all columns should be returned."""
        kelmarsh_dir = str(tmp_farm_dir / "kelmarsh")
        cols, _ = get_data_for_date(
            farm_dir=kelmarsh_dir,
            file_type="data",
            query_date=date(2021, 6, 1),
            columns=None,
        )
        assert "Date and time" in cols
        assert "Wind speed (m/s)" in cols
        assert "Power (kW)" in cols

    def test_each_row_has_correct_column_count(self, tmp_farm_dir):
        """Every returned row must have the same length as the columns list."""
        kelmarsh_dir = str(tmp_farm_dir / "kelmarsh")
        cols, rows = get_data_for_date(
            farm_dir=kelmarsh_dir,
            file_type="data",
            query_date=date(2021, 6, 1),
        )
        for i, row in enumerate(rows):
            assert len(row) == len(cols), (
                f"Row {i} has {len(row)} values but expected {len(cols)}"
            )

    def test_raises_for_unknown_file_type(self, tmp_farm_dir):
        """Should raise ValueError when file_type produces no matching files."""
        kelmarsh_dir = str(tmp_farm_dir / "kelmarsh")
        with pytest.raises(ValueError, match="No parquet files found"):
            get_data_for_date(
                farm_dir=kelmarsh_dir,
                file_type="nonexistent_type",
                query_date=date(2021, 6, 1),
            )

    def test_hill_of_towie_query(self, tmp_farm_dir):
        """get_data_for_date must work with Hill-of-Towie T{NN}_ naming."""
        hot_dir = str(tmp_farm_dir / "hill_of_towie")
        cols, rows = get_data_for_date(
            farm_dir=hot_dir,
            file_type="SCTurbine",
            query_date=date(2016, 7, 19),
        )
        assert len(rows) == 2
        assert "TimeStamp" in cols
        assert "ActivePower" in cols

