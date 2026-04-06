"""
upload_kelmarsh_to_r2.py
========================
Uploads all parquet files from data/kelmarsh/ to the Cloudflare R2 bucket
at https://7cf52f0e0957036ef8b28411ed958be4.r2.cloudflarestorage.com/windfarmdata
under the kelmarsh/ prefix.

Cloudflare R2 is S3-compatible, so boto3 is used with a custom endpoint URL.

Prerequisites
-------------
Set the following environment variables before running:

    $env:R2_ACCESS_KEY_ID     = "<your R2 access key id>"
    $env:R2_SECRET_ACCESS_KEY = "<your R2 secret access key>"

These can be generated in the Cloudflare dashboard under
  R2 → Manage R2 API Tokens → Create API Token (Object Read & Write).

Usage
-----
    python scripts/upload_kelmarsh_to_r2.py

Optional flags
--------------
    --dry-run   Print what would be uploaded without actually uploading.
    --prefix    Override the destination prefix (default: kelmarsh).
"""

import argparse
import os
import sys
import glob
import logging

import boto3
from botocore.exceptions import BotoCoreError, ClientError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

R2_ENDPOINT_URL = "https://7cf52f0e0957036ef8b28411ed958be4.r2.cloudflarestorage.com"
BUCKET_NAME     = "windfarmdata"
DEFAULT_PREFIX  = "kelmarsh"

# Path to local kelmarsh parquet files, relative to the project root
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
LOCAL_DIR    = os.path.join(PROJECT_ROOT, "data", "kelmarsh")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("upload_kelmarsh")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_s3_client():
    """Create a boto3 S3 client pointed at the R2 endpoint."""
    access_key = os.environ.get("R2_ACCESS_KEY_ID")
    secret_key  = os.environ.get("R2_SECRET_ACCESS_KEY")

    if not access_key or not secret_key:
        log.error(
            "R2 credentials not set.\n"
            "Please export R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY "
            "before running this script."
        )
        sys.exit(1)

    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        # R2 does not use AWS regions, but boto3 requires a value
        region_name="auto",
    )


def collect_files(local_dir: str) -> list[str]:
    """Return sorted list of .parquet file paths in local_dir."""
    pattern = os.path.join(local_dir, "*.parquet")
    files   = sorted(glob.glob(pattern))
    if not files:
        log.warning("No .parquet files found in '%s'", local_dir)
    return files


def upload_files(
    s3_client,
    files: list[str],
    bucket: str,
    prefix: str,
    dry_run: bool,
) -> None:
    """Upload each file to bucket/prefix/filename."""
    total   = len(files)
    success = 0
    failed  = []

    for i, local_path in enumerate(files, start=1):
        filename   = os.path.basename(local_path)
        object_key = f"{prefix}/{filename}"
        size_kb    = os.path.getsize(local_path) / 1024

        if dry_run:
            log.info(
                "[DRY RUN] %d/%d  %s  →  s3://%s/%s  (%.1f KB)",
                i, total, filename, bucket, object_key, size_kb,
            )
            success += 1
            continue

        log.info(
            "Uploading %d/%d  %s  →  s3://%s/%s  (%.1f KB)",
            i, total, filename, bucket, object_key, size_kb,
        )
        try:
            s3_client.upload_file(
                Filename=local_path,
                Bucket=bucket,
                Key=object_key,
                ExtraArgs={"ContentType": "application/octet-stream"},
            )
            log.info("  ✓  uploaded successfully")
            success += 1
        except (BotoCoreError, ClientError) as exc:
            log.error("  ✗  failed: %s", exc)
            failed.append(filename)

    # Summary
    log.info("─" * 60)
    log.info(
        "Done — %d/%d file(s) %s",
        success, total,
        "would be uploaded (dry run)" if dry_run else "uploaded successfully",
    )
    if failed:
        log.error("Failed files (%d):", len(failed))
        for f in failed:
            log.error("  • %s", f)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Upload kelmarsh parquet files to Cloudflare R2."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List files that would be uploaded without actually uploading.",
    )
    parser.add_argument(
        "--prefix",
        default=DEFAULT_PREFIX,
        help=f"R2 key prefix / virtual directory (default: {DEFAULT_PREFIX}).",
    )
    args = parser.parse_args()

    # Validate local directory
    if not os.path.isdir(LOCAL_DIR):
        log.error("Local kelmarsh directory not found: '%s'", LOCAL_DIR)
        sys.exit(1)

    files = collect_files(LOCAL_DIR)
    if not files:
        sys.exit(0)

    log.info("Bucket  : %s/%s", BUCKET_NAME, args.prefix)
    log.info("Files   : %d parquet file(s) in '%s'", len(files), LOCAL_DIR)
    if args.dry_run:
        log.info("Mode    : DRY RUN — no files will be uploaded")

    s3 = build_s3_client()
    upload_files(
        s3_client=s3,
        files=files,
        bucket=BUCKET_NAME,
        prefix=args.prefix,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()

