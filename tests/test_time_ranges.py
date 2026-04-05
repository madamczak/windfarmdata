"""
Tests for the GET /wind-farms/time-ranges endpoint.

Verifies that the endpoint:
- Returns HTTP 200
- Returns a 'time_ranges' list with one entry per known farm
- Each entry has the expected fields with correct types
- earliest <= latest when both are present
"""

import pytest
from datetime import datetime
from fastapi.testclient import TestClient


class TestTimeRanges:
    """Tests for GET /wind-farms/time-ranges."""

    def test_returns_200(self, client: TestClient):
        """Endpoint must respond with HTTP 200."""
        response = client.get("/wind-farms/time-ranges")
        assert response.status_code == 200

    def test_response_has_time_ranges_key(self, client: TestClient):
        """Response body must contain a 'time_ranges' list."""
        data = client.get("/wind-farms/time-ranges").json()
        assert "time_ranges" in data
        assert isinstance(data["time_ranges"], list)

    def test_each_entry_has_required_fields(self, client: TestClient):
        """Every entry must have 'farm', 'earliest', 'latest', 'timestamp_column'."""
        data = client.get("/wind-farms/time-ranges").json()
        required = {"farm", "earliest", "latest", "timestamp_column"}
        for entry in data["time_ranges"]:
            missing = required - entry.keys()
            assert not missing, f"Entry {entry} is missing fields: {missing}"

    def test_farm_field_is_string(self, client: TestClient):
        """'farm' field in each entry must be a non-empty string."""
        data = client.get("/wind-farms/time-ranges").json()
        for entry in data["time_ranges"]:
            assert isinstance(entry["farm"], str) and entry["farm"]

    def test_no_duplicate_farms(self, client: TestClient):
        """Each farm directory should appear at most once in the response."""
        data = client.get("/wind-farms/time-ranges").json()
        farms = [e["farm"] for e in data["time_ranges"]]
        assert len(farms) == len(set(farms)), "Duplicate farm entries in time-ranges"

    def test_earliest_before_latest(self, client: TestClient):
        """When both earliest and latest are non-null, earliest must be <= latest."""
        data = client.get("/wind-farms/time-ranges").json()
        for entry in data["time_ranges"]:
            if entry["earliest"] and entry["latest"]:
                earliest = datetime.fromisoformat(entry["earliest"].replace("Z", "+00:00"))
                latest = datetime.fromisoformat(entry["latest"].replace("Z", "+00:00"))
                assert earliest <= latest, (
                    f"Farm '{entry['farm']}': earliest ({earliest}) is after latest ({latest})"
                )

    def test_timestamp_column_is_string_or_null(self, client: TestClient):
        """'timestamp_column' must be a string or null."""
        data = client.get("/wind-farms/time-ranges").json()
        for entry in data["time_ranges"]:
            assert entry["timestamp_column"] is None or isinstance(
                entry["timestamp_column"], str
            ), f"Unexpected type for timestamp_column in {entry}"

    def test_earliest_and_latest_consistent(self, client: TestClient):
        """If timestamp_column is set, both earliest and latest should also be set."""
        data = client.get("/wind-farms/time-ranges").json()
        for entry in data["time_ranges"]:
            if entry["timestamp_column"]:
                assert entry["earliest"] is not None, (
                    f"Farm '{entry['farm']}' has timestamp_column but null earliest"
                )
                assert entry["latest"] is not None, (
                    f"Farm '{entry['farm']}' has timestamp_column but null latest"
                )

