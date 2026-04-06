"""
r2_service.py — Cloudflare R2 integration for the Wind Farm Data API.

Strategy: direct S3 access via DuckDB httpfs.
  • Parquet files are NEVER downloaded to local disk.
  • DuckDB's built-in httpfs extension is used to query files directly from R2
    over S3 protocol.
  • `get_farm_prefix(farm)` returns the S3 prefix URL for a farm, e.g.
    s3://windfarmdata/kelmarsh/
  • `configure_s3_duckdb(conn)` installs httpfs and sets the R2 credentials on
    a DuckDB connection so that read_parquet() calls work against S3 URLs.
  • `list_farm_files(farm, pattern)` uses boto3 (metadata only — no download)
    to enumerate parquet files in a farm prefix.

The rest of the application (query_service, router) calls these helpers instead
of working with local filesystem paths when storage_backend == "r2".
"""

import logging

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from backend.config import settings

logger = logging.getLogger("windfarm.services.r2_service")


def _build_client():
    """Create a boto3 S3 client pointing at the configured R2 endpoint.

    This is used only for listing objects (metadata) — no data is downloaded.
    """
    return boto3.client(
        "s3",
        endpoint_url=settings.r2_endpoint_url,
        aws_access_key_id=settings.r2_access_key_id,
        aws_secret_access_key=settings.r2_secret_access_key,
        region_name="auto",
    )


def configure_s3_duckdb(conn) -> None:
    """Install httpfs and configure S3 credentials on a DuckDB connection.

    Must be called before any read_parquet() query that uses an s3:// URL.
    This is a cheap operation — DuckDB caches the loaded extension in-process.
    """
    try:
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        conn.execute("SET s3_region = 'auto';")
        conn.execute(f"SET s3_endpoint = '{_r2_host()}';")
        conn.execute(f"SET s3_access_key_id = '{settings.r2_access_key_id}';")
        conn.execute(f"SET s3_secret_access_key = '{settings.r2_secret_access_key}';")
        # Force path-style URLs — required by Cloudflare R2
        conn.execute("SET s3_url_style = 'path';")
        logger.debug("configure_s3_duckdb: httpfs configured for R2 endpoint '%s'", _r2_host())
    except Exception as exc:
        logger.error("configure_s3_duckdb: failed to configure httpfs — %s", exc)
        raise


def _r2_host() -> str:
    """Extract the hostname from the R2 endpoint URL (strip https://)."""
    url = settings.r2_endpoint_url.rstrip("/")
    return url.replace("https://", "").replace("http://", "")


def get_farm_prefix(farm: str) -> str:
    """Return the S3 prefix URL for *farm*, e.g. s3://windfarmdata/kelmarsh/.

    This replaces the local directory path used in the 'local' storage backend.
    """
    prefix = f"s3://{settings.r2_bucket_name}/{farm}/"
    logger.debug("get_farm_prefix: farm='%s' → '%s'", farm, prefix)
    return prefix


def list_farm_files(farm: str, pattern: str = "*.parquet") -> list[str]:
    """Return S3 URLs for all parquet files under *farm* in the R2 bucket.

    Uses boto3 list_objects_v2 (metadata only — no data downloaded).
    Results are sorted for deterministic ordering.

    Args:
        farm: Farm directory name, e.g. 'kelmarsh'.
        pattern: Glob-style filename filter (only the basename is matched).
                 Currently supports simple prefix/suffix matching via str.endswith.

    Returns:
        List of S3 URLs such as ['s3://windfarmdata/kelmarsh/data_turbine_1.parquet', ...]
    """
    client = _build_client()
    prefix = f"{farm}/"
    try:
        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=settings.r2_bucket_name, Prefix=prefix)

        urls = []
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                filename = key[len(prefix):]  # strip the farm/ prefix to get bare filename
                if not filename or "/" in filename:
                    # Skip directory placeholders or nested objects
                    continue
                if filename.endswith(".parquet"):
                    url = f"s3://{settings.r2_bucket_name}/{key}"
                    urls.append(url)

        urls.sort()
        logger.debug(
            "list_farm_files: farm='%s' → %d parquet file(s) found", farm, len(urls)
        )
        return urls
    except (BotoCoreError, ClientError) as exc:
        logger.error("list_farm_files: R2 listing failed for farm='%s' — %s", farm, exc)
        return []


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
        logger.debug("list_remote_farms: found prefixes %s", prefixes)
        return prefixes
    except (BotoCoreError, ClientError) as exc:
        logger.error("list_remote_farms: R2 error — %s", exc)
        return []

