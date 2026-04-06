"""
Application configuration using pydantic-settings.
Values can be overridden via environment variables.
"""

import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Global application settings."""

    storage_backend: str = "local"  # "local" | "r2"

    # Base path where per-farm parquet directories live (local mode)
    parquet_base_path: str = os.path.join(
        os.path.dirname(__file__), "..", "data"
    )

    events_db_path: str = os.path.join(
        os.path.dirname(__file__), "..", "data", "events.db"
    )

    aws_region: str = "eu-west-1"

    # ── Cloudflare R2 settings (used when storage_backend = "r2") ──────────
    # Set these via environment variables or a .env file:
    #   R2_ENDPOINT_URL       = https://<account-id>.r2.cloudflarestorage.com
    #   R2_BUCKET_NAME        = windfarmdata
    #   R2_ACCESS_KEY_ID      = <your-key>
    #   R2_SECRET_ACCESS_KEY  = <your-secret>
    r2_endpoint_url: str = "https://7cf52f0e0957036ef8b28411ed958be4.r2.cloudflarestorage.com"
    r2_bucket_name: str = "windfarmdata"
    r2_access_key_id: str = ""
    r2_secret_access_key: str = ""

    # Local directory used to cache files downloaded from R2
    r2_cache_dir: str = os.path.join(
        os.path.dirname(__file__), "..", ".r2_cache"
    )

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Module-level singleton
settings = Settings()

