"""
Tests for backend/services/r2_service.py.

All R2 / boto3 network calls are mocked so no real Cloudflare credentials
or network access are required.
"""

from unittest.mock import MagicMock, patch

import pytest

import backend.services.r2_service as r2_module
from backend.services.r2_service import (
    get_farm_prefix,
    list_farm_files,
    list_remote_farms,
    configure_s3_duckdb,
    _r2_host,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_paginator(objects: list[dict] | None = None):
    """Return a mock paginator that yields one page with *objects*."""
    page_contents = objects or [
        {"Key": "kelmarsh/data_turbine_1.parquet", "Size": 1024},
        {"Key": "kelmarsh/status_turbine_1.parquet", "Size": 512},
    ]
    paginator = MagicMock()
    paginator.paginate.return_value = [{"Contents": page_contents}]
    return paginator


def _make_s3_client(objects: list[dict] | None = None):
    """Return a mock boto3 S3 client."""
    client = MagicMock()
    client.list_objects_v2.return_value = {
        "CommonPrefixes": [{"Prefix": "kelmarsh/"}, {"Prefix": "penmanshiel/"}]
    }
    client.get_paginator.return_value = _make_paginator(objects)
    return client


# ---------------------------------------------------------------------------
# get_farm_prefix
# ---------------------------------------------------------------------------

class TestGetFarmPrefix:
    """Tests for r2_service.get_farm_prefix()."""

    def test_returns_s3_url(self):
        """Should return s3://<bucket>/<farm>/."""
        result = get_farm_prefix("kelmarsh")
        assert result.startswith("s3://")
        assert "kelmarsh" in result
        assert result.endswith("/")

    def test_different_farms_produce_different_prefixes(self):
        """Each farm name should produce a unique prefix."""
        assert get_farm_prefix("kelmarsh") != get_farm_prefix("penmanshiel")


# ---------------------------------------------------------------------------
# _r2_host
# ---------------------------------------------------------------------------

class TestR2Host:
    """Tests for r2_service._r2_host()."""

    def test_strips_https_prefix(self):
        """Should strip https:// from the endpoint URL."""
        host = _r2_host()
        assert not host.startswith("https://")
        assert not host.startswith("http://")

    def test_returns_non_empty_string(self):
        result = _r2_host()
        assert isinstance(result, str) and result


# ---------------------------------------------------------------------------
# list_farm_files
# ---------------------------------------------------------------------------

class TestListFarmFiles:
    """Tests for r2_service.list_farm_files()."""

    def test_returns_s3_urls(self):
        """Each returned path should be a valid s3:// URL."""
        with patch("backend.services.r2_service._build_client") as mock_build:
            mock_build.return_value = _make_s3_client()
            result = list_farm_files("kelmarsh")
        assert all(url.startswith("s3://") for url in result)

    def test_returns_only_parquet_files(self):
        """Non-parquet objects should be filtered out."""
        objects = [
            {"Key": "kelmarsh/README.txt", "Size": 100},
            {"Key": "kelmarsh/data_turbine_1.parquet", "Size": 1024},
            {"Key": "kelmarsh/", "Size": 0},   # directory placeholder
        ]
        with patch("backend.services.r2_service._build_client") as mock_build:
            mock_build.return_value = _make_s3_client(objects)
            result = list_farm_files("kelmarsh")
        assert len(result) == 1
        assert result[0].endswith(".parquet")

    def test_returns_sorted_list(self):
        """Results should be sorted for deterministic ordering."""
        objects = [
            {"Key": "kelmarsh/data_turbine_2.parquet", "Size": 512},
            {"Key": "kelmarsh/data_turbine_1.parquet", "Size": 512},
        ]
        with patch("backend.services.r2_service._build_client") as mock_build:
            mock_build.return_value = _make_s3_client(objects)
            result = list_farm_files("kelmarsh")
        assert result == sorted(result)

    def test_returns_empty_list_on_client_error(self):
        """Should return [] on boto3 error, not raise."""
        from botocore.exceptions import ClientError
        with patch("backend.services.r2_service._build_client") as mock_build:
            client = MagicMock()
            client.get_paginator.side_effect = ClientError(
                {"Error": {"Code": "NoSuchBucket", "Message": "x"}}, "ListObjectsV2"
            )
            mock_build.return_value = client
            result = list_farm_files("kelmarsh")
        assert result == []

    def test_skips_nested_objects(self):
        """Objects with a slash after the farm prefix should be skipped."""
        objects = [
            {"Key": "kelmarsh/sub/data_turbine_1.parquet", "Size": 1024},
            {"Key": "kelmarsh/data_turbine_1.parquet", "Size": 1024},
        ]
        with patch("backend.services.r2_service._build_client") as mock_build:
            mock_build.return_value = _make_s3_client(objects)
            result = list_farm_files("kelmarsh")
        assert len(result) == 1


# ---------------------------------------------------------------------------
# list_remote_farms
# ---------------------------------------------------------------------------

class TestListRemoteFarms:
    """Tests for r2_service.list_remote_farms()."""

    def test_returns_farm_names(self):
        """Should return the prefix names with trailing slash stripped."""
        with patch("backend.services.r2_service._build_client") as mock_build:
            mock_build.return_value = _make_s3_client()
            result = list_remote_farms()
        assert "kelmarsh" in result
        assert "penmanshiel" in result

    def test_strips_trailing_slash(self):
        """Farm names returned should not contain a trailing slash."""
        with patch("backend.services.r2_service._build_client") as mock_build:
            mock_build.return_value = _make_s3_client()
            result = list_remote_farms()
        assert all(not name.endswith("/") for name in result)

    def test_returns_empty_on_client_error(self):
        """Should return [] on boto3 error, not raise."""
        from botocore.exceptions import ClientError
        with patch("backend.services.r2_service._build_client") as mock_build:
            client = MagicMock()
            client.list_objects_v2.side_effect = ClientError(
                {"Error": {"Code": "NoSuchBucket", "Message": "x"}}, "ListObjectsV2"
            )
            mock_build.return_value = client
            result = list_remote_farms()
        assert result == []


# ---------------------------------------------------------------------------
# configure_s3_duckdb
# ---------------------------------------------------------------------------

class TestConfigureS3DuckDB:
    """Tests for r2_service.configure_s3_duckdb()."""

    def test_executes_required_statements(self):
        """Should execute INSTALL httpfs, LOAD httpfs and SET statements."""
        conn = MagicMock()
        configure_s3_duckdb(conn)
        calls = [str(c) for c in conn.execute.call_args_list]
        full_sql = " ".join(calls).lower()
        assert "httpfs" in full_sql
        assert "s3_access_key_id" in full_sql
        assert "s3_secret_access_key" in full_sql

    def test_raises_on_duckdb_error(self):
        """Should propagate exceptions from DuckDB execute calls."""
        conn = MagicMock()
        conn.execute.side_effect = RuntimeError("duckdb error")
        with pytest.raises(RuntimeError):
            configure_s3_duckdb(conn)
