"""
Tests for the GET /wind-farms/columns endpoint.

Verifies that the endpoint:
- Returns HTTP 200
- Returns a 'farms' list with expected structure
- Each farm entry has 'farm' string and 'columns_by_type' dict
- Column lists are non-empty and contain only strings
- Known file types are present for each farm
"""

import pytest
from fastapi.testclient import TestClient


# Known file-type groups per farm.  These are the minimum expected keys; the
# actual response may include more.
EXPECTED_TYPES = {
    "kelmarsh": {"data", "status"},
    "penmanshiel": {"data", "status"},
}

# Hill of Towie uses sensor-table naming — just verify a non-empty dict.
HOT_FARM = "hill_of_towie"


class TestColumns:
    """Tests for GET /wind-farms/columns."""

    def test_returns_200(self, client: TestClient):
        """Endpoint must respond with HTTP 200."""
        response = client.get("/wind-farms/columns")
        assert response.status_code == 200

    def test_response_has_farms_key(self, client: TestClient):
        """Response must contain a 'farms' list."""
        data = client.get("/wind-farms/columns").json()
        assert "farms" in data
        assert isinstance(data["farms"], list)

    def test_each_entry_has_required_fields(self, client: TestClient):
        """Every farm entry must have 'farm' and 'columns_by_type'."""
        data = client.get("/wind-farms/columns").json()
        for entry in data["farms"]:
            assert "farm" in entry, f"Missing 'farm' in {entry}"
            assert "columns_by_type" in entry, f"Missing 'columns_by_type' in {entry}"

    def test_farm_field_is_non_empty_string(self, client: TestClient):
        """'farm' field must be a non-empty string."""
        data = client.get("/wind-farms/columns").json()
        for entry in data["farms"]:
            assert isinstance(entry["farm"], str) and entry["farm"]

    def test_columns_by_type_is_dict(self, client: TestClient):
        """'columns_by_type' must be a dict of str → list."""
        data = client.get("/wind-farms/columns").json()
        for entry in data["farms"]:
            cbt = entry["columns_by_type"]
            assert isinstance(cbt, dict), f"columns_by_type is not a dict in {entry['farm']}"
            for ftype, cols in cbt.items():
                assert isinstance(ftype, str), f"File type key is not a string: {ftype}"
                assert isinstance(cols, list), (
                    f"Column list for '{ftype}' in '{entry['farm']}' is not a list"
                )

    def test_column_names_are_strings(self, client: TestClient):
        """Each column name in every column list must be a string."""
        data = client.get("/wind-farms/columns").json()
        for entry in data["farms"]:
            for ftype, cols in entry["columns_by_type"].items():
                for col in cols:
                    assert isinstance(col, str), (
                        f"Non-string column name '{col}' in farm '{entry['farm']}' "
                        f"file type '{ftype}'"
                    )

    def test_column_lists_are_non_empty(self, client: TestClient):
        """Each file-type group must have at least one column."""
        data = client.get("/wind-farms/columns").json()
        for entry in data["farms"]:
            for ftype, cols in entry["columns_by_type"].items():
                assert len(cols) > 0, (
                    f"Empty column list for file type '{ftype}' in farm '{entry['farm']}'"
                )

    def test_kelmarsh_and_penmanshiel_have_data_and_status_types(
        self, client: TestClient
    ):
        """Kelmarsh and Penmanshiel must expose at least 'data' and 'status' file types."""
        data = client.get("/wind-farms/columns").json()
        farm_map = {e["farm"]: e["columns_by_type"] for e in data["farms"]}

        for farm, expected in EXPECTED_TYPES.items():
            if farm not in farm_map:
                pytest.skip(f"Farm '{farm}' not present in response — skipping")
            actual_types = set(farm_map[farm].keys())
            missing = expected - actual_types
            assert not missing, (
                f"Farm '{farm}' is missing file types: {missing}"
            )

    def test_hill_of_towie_has_at_least_one_file_type(self, client: TestClient):
        """Hill of Towie must expose at least one sensor file type."""
        data = client.get("/wind-farms/columns").json()
        farm_map = {e["farm"]: e["columns_by_type"] for e in data["farms"]}

        if HOT_FARM not in farm_map:
            pytest.skip("Hill of Towie not present in response — skipping")

        assert len(farm_map[HOT_FARM]) > 0, (
            "Hill of Towie has no file types in columns_by_type"
        )

    def test_no_duplicate_farm_entries(self, client: TestClient):
        """Farm directory names in the response must be unique."""
        data = client.get("/wind-farms/columns").json()
        farms = [e["farm"] for e in data["farms"]]
        assert len(farms) == len(set(farms)), "Duplicate farm entries in /columns response"

