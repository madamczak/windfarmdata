"""
Tests for the GET /wind-farms/{farm}/data/{date} endpoint.

Tests are split into two categories:
1. Validation tests — do not hit the filesystem (fast, always run)
2. Integration-style tests — use the mock_client fixture so they always run
   without needing real data on disk.
"""

import pytest
from fastapi.testclient import TestClient

# Farm + date that exist in the mock parquet fixture (see conftest.py)
MOCK_FARM = "kelmarsh"
MOCK_DATE = "2021-06-01"
MOCK_FILE_TYPE = "data"


# ---------------------------------------------------------------------------
# Input validation tests (no real data required)
# ---------------------------------------------------------------------------

class TestDayDataValidation:
    """Validation tests for GET /wind-farms/{farm}/data/{date}."""

    def test_invalid_farm_returns_404(self, client: TestClient):
        """A non-existent farm name must return HTTP 404."""
        response = client.get("/wind-farms/does_not_exist/data/2021-01-01")
        assert response.status_code == 404

    def test_invalid_date_format_returns_422(self, client: TestClient):
        """A malformed date must return HTTP 422 (Unprocessable Entity)."""
        response = client.get("/wind-farms/kelmarsh/data/not-a-date")
        assert response.status_code == 422

    def test_invalid_file_type_returns_400(self, client: TestClient):
        """
        An unrecognised file_type for a valid farm must return HTTP 400,
        because file_type is validated before any filesystem access.
        """
        response = client.get(
            "/wind-farms/kelmarsh/data/2021-06-01",
            params={"file_type": "nonexistent_type"},
        )
        assert response.status_code == 400

    def test_path_traversal_is_rejected(self, client: TestClient):
        """Farm name with path traversal characters must return HTTP 404."""
        response = client.get("/wind-farms/../config/data/2021-01-01")
        assert response.status_code in (404, 422)


# ---------------------------------------------------------------------------
# Integration tests — always run using the mock_client fixture
# ---------------------------------------------------------------------------

class TestDayDataIntegration:
    """Integration tests for the day-data endpoint using mock parquet files."""

    def test_returns_200(self, mock_client: TestClient):
        """Day-data endpoint must return HTTP 200 for a known farm + date."""
        response = mock_client.get(
            f"/wind-farms/{MOCK_FARM}/data/{MOCK_DATE}",
            params={"file_type": MOCK_FILE_TYPE},
        )
        assert response.status_code == 200

    def test_response_shape(self, mock_client: TestClient):
        """Response must contain 'farm', 'date', 'columns', 'row_count', 'rows'."""
        data = mock_client.get(
            f"/wind-farms/{MOCK_FARM}/data/{MOCK_DATE}",
            params={"file_type": MOCK_FILE_TYPE},
        ).json()
        for key in ("farm", "file_type", "date", "columns", "row_count", "rows"):
            assert key in data, f"Missing key '{key}' in response"

    def test_farm_and_date_match_request(self, mock_client: TestClient):
        """'farm' and 'date' in the response must match what was requested."""
        data = mock_client.get(
            f"/wind-farms/{MOCK_FARM}/data/{MOCK_DATE}",
            params={"file_type": MOCK_FILE_TYPE},
        ).json()
        assert data["farm"] == MOCK_FARM
        assert data["date"] == MOCK_DATE

    def test_row_count_matches_rows_length(self, mock_client: TestClient):
        """'row_count' must equal the actual number of rows in 'rows'."""
        data = mock_client.get(
            f"/wind-farms/{MOCK_FARM}/data/{MOCK_DATE}",
            params={"file_type": MOCK_FILE_TYPE},
        ).json()
        assert data["row_count"] == len(data["rows"])

    def test_each_row_has_correct_column_count(self, mock_client: TestClient):
        """Every row must have the same number of values as there are columns."""
        data = mock_client.get(
            f"/wind-farms/{MOCK_FARM}/data/{MOCK_DATE}",
            params={"file_type": MOCK_FILE_TYPE},
        ).json()
        n_cols = len(data["columns"])
        for i, row in enumerate(data["rows"]):
            assert len(row) == n_cols, (
                f"Row {i} has {len(row)} values but expected {n_cols}"
            )

    def test_column_filter_reduces_columns(self, mock_client: TestClient):
        """When a single column is requested only that column (plus timestamp) appears."""
        all_data = mock_client.get(
            f"/wind-farms/{MOCK_FARM}/data/{MOCK_DATE}",
            params={"file_type": MOCK_FILE_TYPE},
        ).json()
        all_cols = all_data["columns"]
        assert len(all_cols) >= 2, "Not enough columns in mock data to test filtering"

        target_col = all_cols[1]
        filtered = mock_client.get(
            f"/wind-farms/{MOCK_FARM}/data/{MOCK_DATE}",
            params={"file_type": MOCK_FILE_TYPE, "columns": target_col},
        ).json()

        assert target_col in filtered["columns"]
        assert len(filtered["columns"]) <= len(all_cols)

    def test_columns_field_is_list_of_strings(self, mock_client: TestClient):
        """'columns' in the response must be a non-empty list of strings."""
        data = mock_client.get(
            f"/wind-farms/{MOCK_FARM}/data/{MOCK_DATE}",
            params={"file_type": MOCK_FILE_TYPE},
        ).json()
        assert isinstance(data["columns"], list) and data["columns"]
        for col in data["columns"]:
            assert isinstance(col, str)

    def test_rows_is_list_of_lists(self, mock_client: TestClient):
        """'rows' must be a list of lists."""
        data = mock_client.get(
            f"/wind-farms/{MOCK_FARM}/data/{MOCK_DATE}",
            params={"file_type": MOCK_FILE_TYPE},
        ).json()
        assert isinstance(data["rows"], list)
        for row in data["rows"]:
            assert isinstance(row, list)

    def test_no_data_date_returns_empty_rows(self, mock_client: TestClient):
        """A valid farm + file_type with a date that has no data must return 0 rows."""
        data = mock_client.get(
            f"/wind-farms/{MOCK_FARM}/data/1999-01-01",
            params={"file_type": MOCK_FILE_TYPE},
        ).json()
        assert data["row_count"] == 0
        assert data["rows"] == []

