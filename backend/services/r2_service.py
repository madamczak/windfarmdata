"""
r2_service.py — Cloudflare R2 integration for the Wind Farm Data API.

Strategy: download-and-cache.
  • On first access for a farm, all parquet files for that farm are downloaded
    from R2 into a local cache directory (.r2_cache/<farm>/).
  • On subsequent requests the cached files are used directly, so DuckDB
    queries are always run against local paths — no streaming required.
  • The cache is valid for the lifetime of the process. Restart the server
    to force a re-download.

The rest of the application (query_service, router) is completely unaware of
R2 — they just receive a local directory path and work normally.
"""

import logging
import os

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from backend.config import settings

logger = logging.getLogger("windfarm.services.r2_service")

# In-process set of farm prefixes that have already been synced this session
_synced: set[str] = set()


def _build_client():
    """Create a boto3 S3 client pointing at the configured R2 endpoint."""
    return boto3.client(
        "s3",
        endpoint_url=settings.r2_endpoint_url,
        aws_access_key_id=settings.r2_access_key_id,
        aws_secret_access_key=settings.r2_secret_access_key,
        region_name="auto",
    )


def get_farm_dir(farm: str) -> str:
    """Return the local directory path for *farm*, downloading from R2 if needed.

    This is the single entry point used by the router / query_service.
    Returns a path to a local directory that contains the farm's parquet files,
    identical to what the local storage backend would return.
    """
    cache_dir = os.path.abspath(settings.r2_cache_dir)
    local_farm_dir = os.path.join(cache_dir, farm)

    if farm in _synced:
        logger.debug("r2_service.get_farm_dir: cache hit for farm='%s'", farm)
        return local_farm_dir

    logger.info("r2_service.get_farm_dir: syncing farm='%s' from R2", farm)
    _sync_farm(farm, local_farm_dir)
    _synced.add(farm)
    return local_farm_dir


def list_remote_farms() -> list[str]:
    """Return farm prefixes (top-level 'directories') that exist in the R2 bucket."""
    client = _build_client()
    try:
        resp = client.list_objects_v2(
            Bucket=settings.r2_bucket_name,
            Delimiter="/",
        )
        prefixes = [
            p["Prefix"].rstrip("/")
            for p in resp.get("CommonPrefixes", [])
        ]
        logger.debug("r2_service.list_remote_farms: found prefixes %s", prefixes)
        return prefixes
    except (BotoCoreError, ClientError) as exc:
        logger.error("r2_service.list_remote_farms: R2 error — %s", exc)
        return []


def _sync_farm(farm: str, local_dir: str) -> None:
    """Download all parquet files for *farm* from R2 into *local_dir*."""
    os.makedirs(local_dir, exist_ok=True)
    client = _build_client()
    prefix = f"{farm}/"

    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=settings.r2_bucket_name, Prefix=prefix)

    downloaded = 0
    skipped = 0

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            filename = os.path.basename(key)

            # Only sync parquet files; skip 'directory' placeholder objects
            if not filename.endswith(".parquet"):
                continue

            local_path = os.path.join(local_dir, filename)

            # Skip if already cached (size match is a cheap freshness check)
            if os.path.exists(local_path) and os.path.getsize(local_path) == obj["Size"]:
                logger.debug("r2_service._sync_farm: skip (cached) '%s'", filename)
                skipped += 1
                continue

            logger.info(
                "r2_service._sync_farm: downloading '%s'  (%.1f KB)",
                key, obj["Size"] / 1024,
            )
            try:
                client.download_file(
                    Bucket=settings.r2_bucket_name,
                    Key=key,
                    Filename=local_path,
                )
                downloaded += 1
            except (BotoCoreError, ClientError) as exc:
                logger.error(
                    "r2_service._sync_farm: failed to download '%s' — %s", key, exc
                )

    logger.info(
        "r2_service._sync_farm: farm='%s' downloaded=%d skipped=%d",
        farm, downloaded, skipped,
    )

