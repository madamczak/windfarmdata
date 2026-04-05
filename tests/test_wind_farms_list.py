"""
Tests for the GET /wind-farms endpoint.

Verifies that the endpoint:
- Returns HTTP 200
- Returns valid JSON with the expected top-level keys
- Returns at most 3 wind farms (Kelmarsh, Penmanshiel, Hill of Towie)
- Each farm entry has the required fields and correct types
- turbine_count is a non-negative integer
- 'total' matches the length of 'wind_farms'
"""

import pytest
from fastapi.testclient import TestClient


KNOWN_FARM_NAMES = {"Kelmarsh", "Penmanshiel", "Hill of Towie"}
KNOWN_FARM_DIRS = {"kelmarsh", "penmanshiel", "hill_of_towie"}


class TestListWindFarms:
    """Tests for GET /wind-farms."""

    def test_returns_200(self, client: TestClient):
        """Endpoint must respond with HTTP 200."""
        response = client.get("/wind-farms")
        assert response.status_code == 200

    def test_response_shape(self, client: TestClient):
        """Response body must contain 'wind_farms' list and 'total' int."""
        data = client.get("/wind-farms").json()
        assert "wind_farms" in data
        assert "total" in data
        assert isinstance(data["wind_farms"], list)
        assert isinstance(data["total"], int)

    def test_total_matches_list_length(self, client: TestClient):
        """'total' field must equal the length of the 'wind_farms' list."""
        data = client.get("/wind-farms").json()
        assert data["total"] == len(data["wind_farms"])

    def test_each_farm_has_required_fields(self, client: TestClient):
        """Every farm entry must have 'name', 'directory', and 'turbine_count'."""
        data = client.get("/wind-farms").json()
        for farm in data["wind_farms"]:
            assert "name" in farm, f"Missing 'name' in {farm}"
            assert "directory" in farm, f"Missing 'directory' in {farm}"
            assert "turbine_count" in farm, f"Missing 'turbine_count' in {farm}"

    def test_farm_names_are_strings(self, client: TestClient):
        """'name' and 'directory' fields must be non-empty strings."""
        data = client.get("/wind-farms").json()
        for farm in data["wind_farms"]:
            assert isinstance(farm["name"], str) and farm["name"]
            assert isinstance(farm["directory"], str) and farm["directory"]

    def test_turbine_count_is_non_negative(self, client: TestClient):
        """'turbine_count' must be a non-negative integer."""
        data = client.get("/wind-farms").json()
        for farm in data["wind_farms"]:
            assert isinstance(farm["turbine_count"], int)
            assert farm["turbine_count"] >= 0

    def test_farm_names_are_known(self, client: TestClient):
        """All returned farm names must be from the known set."""
        data = client.get("/wind-farms").json()
        for farm in data["wind_farms"]:
            assert farm["name"] in KNOWN_FARM_NAMES, (
                f"Unexpected farm name: {farm['name']}"
            )

    def test_farm_directories_are_known(self, client: TestClient):
        """All returned directory names must be from the known set."""
        data = client.get("/wind-farms").json()
        for farm in data["wind_farms"]:
            assert farm["directory"] in KNOWN_FARM_DIRS, (
                f"Unexpected farm directory: {farm['directory']}"
            )

    def test_no_duplicate_names(self, client: TestClient):
        """Wind farm names in the response must be unique."""
        data = client.get("/wind-farms").json()
        names = [f["name"] for f in data["wind_farms"]]
        assert len(names) == len(set(names)), "Duplicate wind farm names found"

    def test_no_duplicate_directories(self, client: TestClient):
        """Wind farm directory names in the response must be unique."""
        data = client.get("/wind-farms").json()
        dirs = [f["directory"] for f in data["wind_farms"]]
        assert len(dirs) == len(set(dirs)), "Duplicate farm directories found"

