"""
Tests for backend/services/r2_service.py and the _resolve_farm_dir helper
in backend/routers/wind_farms.py.

All R2 network calls are mocked via unittest.mock so no real Cloudflare
credentials or network access are required.
"""

import pytest  # noqa: F401 — needed for tmp_path fixture injection
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

from backend import config as backend_config
from backend.main import app
import backend.services.r2_service as r2_module
from backend.services.r2_service import (
    get_farm_dir,
    list_remote_farms,
    _sync_farm,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_s3_client_mock(objects: list[dict] | None = None):
    """Return a mock boto3 S3 client that lists the given objects."""
    client = MagicMock()

    # list_objects_v2 — used by list_remote_farms
    client.list_objects_v2.return_value = {
        "CommonPrefixes": [{"Prefix": "kelmarsh/"}, {"Prefix": "penmanshiel/"}]
    }

    # paginate — used by _sync_farm
    page_contents = objects or [
        {"Key": "kelmarsh/data_turbine_1.parquet", "Size": 1024},
        {"Key": "kelmarsh/status_turbine_1.parquet", "Size": 512},
    ]
    paginator = MagicMock()
    paginator.paginate.return_value = [{"Contents": page_contents}]
    client.get_paginator.return_value = paginator

    return client


# ---------------------------------------------------------------------------
# list_remote_farms
# ---------------------------------------------------------------------------

class TestListRemoteFarms:
    """Tests for r2_service.list_remote_farms()."""

    def test_returns_list_of_farm_names(self):
        """Should return the prefix names with trailing slash stripped."""
        with patch("backend.services.r2_service._build_client") as mock_build:
            mock_build.return_value = _make_s3_client_mock()
            result = list_remote_farms()
        assert "kelmarsh" in result
        assert "penmanshiel" in result

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
# _sync_farm
# ---------------------------------------------------------------------------

class TestSyncFarm:
    """Tests for r2_service._sync_farm()."""

    def test_downloads_parquet_files(self, tmp_path):
        """Should call download_file for each .parquet object."""
        objects = [
            {"Key": "kelmarsh/data_turbine_1.parquet", "Size": 1024},
            {"Key": "kelmarsh/status_turbine_1.parquet", "Size": 512},
        ]
        with patch("backend.services.r2_service._build_client") as mock_build:
            mock_client = _make_s3_client_mock(objects)
            mock_build.return_value = mock_client
            _sync_farm("kelmarsh", str(tmp_path / "kelmarsh"))

        assert mock_client.download_file.call_count == 2

    def test_skips_non_parquet_objects(self, tmp_path):
        """Objects without .parquet extension should not be downloaded."""
        objects = [
            {"Key": "kelmarsh/", "Size": 0},               # directory placeholder
            {"Key": "kelmarsh/README.txt", "Size": 100},   # non-parquet
            {"Key": "kelmarsh/data_turbine_1.parquet", "Size": 1024},
        ]
        with patch("backend.services.r2_service._build_client") as mock_build:
            mock_client = _make_s3_client_mock(objects)
            mock_build.return_value = mock_client
            _sync_farm("kelmarsh", str(tmp_path / "kelmarsh"))

        assert mock_client.download_file.call_count == 1

    def test_skips_already_cached_files(self, tmp_path):
        """Files already in cache with matching size should not be re-downloaded."""
        farm_dir = tmp_path / "kelmarsh"
        farm_dir.mkdir()
        cached = farm_dir / "data_turbine_1.parquet"
        cached.write_bytes(b"x" * 1024)  # size matches

        objects = [{"Key": "kelmarsh/data_turbine_1.parquet", "Size": 1024}]
        with patch("backend.services.r2_service._build_client") as mock_build:
            mock_client = _make_s3_client_mock(objects)
            mock_build.return_value = mock_client
            _sync_farm("kelmarsh", str(farm_dir))

        mock_client.download_file.assert_not_called()

    def test_download_error_is_logged_not_raised(self, tmp_path):
        """A failed download should log the error and continue, not raise."""
        from botocore.exceptions import ClientError
        objects = [
            {"Key": "kelmarsh/data_turbine_1.parquet", "Size": 1024},
            {"Key": "kelmarsh/data_turbine_2.parquet", "Size": 1024},
        ]
        with patch("backend.services.r2_service._build_client") as mock_build:
            mock_client = _make_s3_client_mock(objects)
            mock_client.download_file.side_effect = ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "x"}}, "GetObject"
            )
            mock_build.return_value = mock_client
            # Should not raise
            _sync_farm("kelmarsh", str(tmp_path / "kelmarsh"))


# ---------------------------------------------------------------------------
# get_farm_dir — cache behaviour
# ---------------------------------------------------------------------------

class TestGetFarmDir:
    """Tests for r2_service.get_farm_dir()."""

    def setup_method(self):
        """Clear the in-process sync cache before each test."""
        r2_module._synced.clear()

    def test_returns_local_path_under_cache_dir(self, tmp_path):
        """get_farm_dir should return a path inside r2_cache_dir."""
        original_cache = backend_config.settings.r2_cache_dir
        backend_config.settings.r2_cache_dir = str(tmp_path)
        try:
            with patch("backend.services.r2_service._sync_farm"):
                result = get_farm_dir("kelmarsh")
            assert result == str(tmp_path / "kelmarsh")
        finally:
            backend_config.settings.r2_cache_dir = original_cache

    def test_syncs_on_first_access(self, tmp_path):
        """_sync_farm should be called the first time a farm is requested."""
        original_cache = backend_config.settings.r2_cache_dir
        backend_config.settings.r2_cache_dir = str(tmp_path)
        try:
            with patch("backend.services.r2_service._sync_farm") as mock_sync:
                get_farm_dir("kelmarsh")
            mock_sync.assert_called_once()
        finally:
            backend_config.settings.r2_cache_dir = original_cache

    def test_does_not_sync_on_second_access(self, tmp_path):
        """_sync_farm should NOT be called again for an already-cached farm."""
        original_cache = backend_config.settings.r2_cache_dir
        backend_config.settings.r2_cache_dir = str(tmp_path)
        try:
            with patch("backend.services.r2_service._sync_farm") as mock_sync:
                get_farm_dir("kelmarsh")
                get_farm_dir("kelmarsh")
            assert mock_sync.call_count == 1
        finally:
            backend_config.settings.r2_cache_dir = original_cache


# ---------------------------------------------------------------------------
# _resolve_farm_dir — router branch for R2
# ---------------------------------------------------------------------------

class TestResolveFarmDirR2:
    """Tests for the _resolve_farm_dir helper when storage_backend == 'r2'."""

    def test_r2_backend_calls_get_farm_dir(self, tmp_farm_dir):
        """When storage_backend='r2', endpoints should use r2_service.get_farm_dir."""
        r2_module._synced.clear()
        original_backend = backend_config.settings.storage_backend
        original_cache   = backend_config.settings.r2_cache_dir
        # Point r2 cache at our mock farm data so the endpoint can work
        backend_config.settings.storage_backend = "r2"
        backend_config.settings.r2_cache_dir    = str(tmp_farm_dir)
        try:
            with patch("backend.services.r2_service._sync_farm"):
                client = TestClient(app)
                response = client.get("/wind-farms")
            assert response.status_code == 200
            farms = {f["directory"] for f in response.json()["wind_farms"]}
            assert "kelmarsh" in farms
        finally:
            backend_config.settings.storage_backend = original_backend
            backend_config.settings.r2_cache_dir    = original_cache
            r2_module._synced.clear()

