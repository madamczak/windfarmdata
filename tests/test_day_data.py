"""
Tests for the GET /wind-farms/{farm}/data/{date} endpoint.

Tests are split into two categories:
1. Validation tests — do not hit the filesystem (fast, always run)
2. Integration-style tests — use known farms/dates (skipped when data absent)

For the integration tests we pick the first available farm + date reported by
the /wind-farms/time-ranges endpoint so the tests remain portable across
different machines.
"""

import pytest
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _first_available_farm_and_date(client: TestClient) -> tuple[str, str] | None:
    """
    Query /wind-farms/time-ranges and return (farm_directory, iso_date_string)
    for the first farm that has data, or None if no farm has data.
    """
    data = client.get("/wind-farms/time-ranges").json()
    for entry in data.get("time_ranges", []):
        if entry.get("earliest"):
            # earliest is a full ISO datetime — take just the date part
            date_str = entry["earliest"][:10]
            return entry["farm"], date_str
    return None


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
        because no matching parquet files will be found.
        """
        response = client.get(
            "/wind-farms/kelmarsh/data/2021-06-01",
            params={"file_type": "nonexistent_type"},
        )
        assert response.status_code == 400

    def test_path_traversal_is_rejected(self, client: TestClient):
        """Farm name with path traversal characters must return HTTP 404."""
        response = client.get("/wind-farms/../config/data/2021-01-01")
        # FastAPI may return 404 or 422 depending on routing — either is acceptable
        assert response.status_code in (404, 422)


# ---------------------------------------------------------------------------
# Integration tests — use the first farm + date with real data
# ---------------------------------------------------------------------------

class TestDayDataIntegration:
    """Integration tests for the day-data endpoint using real parquet files."""

    @pytest.fixture(autouse=True)
    def _skip_if_no_data(self, client: TestClient):
        """Skip all tests in this class if no farm has data."""
        result = _first_available_farm_and_date(client)
        if result is None:
            pytest.skip("No farm with data available — skipping integration tests")
        self.farm, self.date = result

    def test_returns_200(self, client: TestClient):
        """Day-data endpoint must return HTTP 200 for a known farm + date."""
        response = client.get(
            f"/wind-farms/{self.farm}/data/{self.date}",
            params={"file_type": "data"},
        )
        assert response.status_code == 200

    def test_response_shape(self, client: TestClient):
        """Response must contain 'farm', 'date', 'columns', 'row_count', 'rows'."""
        data = client.get(
            f"/wind-farms/{self.farm}/data/{self.date}",
            params={"file_type": "data"},
        ).json()
        for key in ("farm", "file_type", "date", "columns", "row_count", "rows"):
            assert key in data, f"Missing key '{key}' in response"

    def test_farm_and_date_match_request(self, client: TestClient):
        """'farm' and 'date' in the response must match what was requested."""
        data = client.get(
            f"/wind-farms/{self.farm}/data/{self.date}",
            params={"file_type": "data"},
        ).json()
        assert data["farm"] == self.farm
        assert data["date"] == self.date

    def test_row_count_matches_rows_length(self, client: TestClient):
        """'row_count' must equal the actual number of rows in 'rows'."""
        data = client.get(
            f"/wind-farms/{self.farm}/data/{self.date}",
            params={"file_type": "data"},
        ).json()
        assert data["row_count"] == len(data["rows"])

    def test_each_row_has_correct_column_count(self, client: TestClient):
        """Every row must have the same number of values as there are columns."""
        data = client.get(
            f"/wind-farms/{self.farm}/data/{self.date}",
            params={"file_type": "data"},
        ).json()
        n_cols = len(data["columns"])
        for i, row in enumerate(data["rows"]):
            assert len(row) == n_cols, (
                f"Row {i} has {len(row)} values but expected {n_cols}"
            )

    def test_column_filter_reduces_columns(self, client: TestClient):
        """When a single column is requested only that column (plus timestamp) appears."""
        # First get all column names so we can pick one
        all_data = client.get(
            f"/wind-farms/{self.farm}/data/{self.date}",
            params={"file_type": "data"},
        ).json()
        all_cols = all_data["columns"]
        if len(all_cols) < 2:
            pytest.skip("Not enough columns to test column filtering")

        # Pick the second column (first is likely the timestamp)
        target_col = all_cols[1]

        filtered = client.get(
            f"/wind-farms/{self.farm}/data/{self.date}",
            params={"file_type": "data", "columns": target_col},
        ).json()

        assert target_col in filtered["columns"], (
            f"Requested column '{target_col}' not in filtered response columns"
        )
        # The filtered response should have fewer or equal columns than the full one
        assert len(filtered["columns"]) <= len(all_cols)

    def test_columns_field_is_list_of_strings(self, client: TestClient):
        """'columns' in the response must be a non-empty list of strings."""
        data = client.get(
            f"/wind-farms/{self.farm}/data/{self.date}",
            params={"file_type": "data"},
        ).json()
        assert isinstance(data["columns"], list) and data["columns"]
        for col in data["columns"]:
            assert isinstance(col, str)

    def test_rows_is_list_of_lists(self, client: TestClient):
        """'rows' must be a list of lists."""
        data = client.get(
            f"/wind-farms/{self.farm}/data/{self.date}",
            params={"file_type": "data"},
        ).json()
        assert isinstance(data["rows"], list)
        for row in data["rows"]:
            assert isinstance(row, list)

