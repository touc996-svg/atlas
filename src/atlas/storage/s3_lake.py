"""Idempotent publisher for ATLAS partitioned Parquet datasets.

Existing object keys are preserved. The publisher only uploads Parquet part files
whose destination key is not already present in the target S3 prefix.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

DEFAULT_ENDPOINT = "http://localhost:9000"
DEFAULT_BUCKET = "atlas-lake"
DEFAULT_SOURCE = Path("data/processed/events")
DEFAULT_PREFIX = "silver/events"


def ensure_bucket(s3: Any, bucket: str) -> None:
    """Create the target bucket only when it is confirmed missing."""
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as error:
        response = error.response or {}
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        code = str(response.get("Error", {}).get("Code", ""))
        missing = status == 404 or code in {"404", "NoSuchBucket", "NotFound"}
        if not missing:
            raise
        s3.create_bucket(Bucket=bucket)


def list_existing_keys(s3: Any, bucket: str, prefix: str) -> set[str]:
    """List all object keys under prefix, including every continuation page.

    Listing fails closed: if the object store cannot be listed, no uploads begin.
    This avoids accidentally overwriting existing objects after a partial listing.
    """
    paginator = s3.get_paginator("list_objects_v2")
    keys: set[str] = set()
    normalized_prefix = prefix.strip("/")
    request_prefix = f"{normalized_prefix}/" if normalized_prefix else ""
    for page in paginator.paginate(Bucket=bucket, Prefix=request_prefix):
        keys.update(
            item["Key"]
            for item in page.get("Contents", [])
            if isinstance(item, dict) and isinstance(item.get("Key"), str)
        )
    return keys


def sync_parquet_dataset(
    s3: Any,
    *,
    source: Path,
    bucket: str,
    prefix: str = DEFAULT_PREFIX,
) -> dict[str, Any]:
    """Upload only Parquet objects absent from the destination prefix.

    The Spark incremental writer creates unique part-file names and appends them
    to existing partition directories. Existing keys are therefore immutable
    inputs to this publisher and are never downloaded, overwritten, or deleted.
    """
    source = Path(source)
    files = sorted(path for path in source.rglob("*.parquet") if path.is_file())
    if not files:
        raise FileNotFoundError(f"No Parquet files found in {source}")

    existing = list_existing_keys(s3, bucket, prefix)
    normalized_prefix = prefix.strip("/")
    uploaded: list[str] = []
    skipped: list[str] = []

    for file_path in files:
        relative = file_path.relative_to(source).as_posix()
        key = f"{normalized_prefix}/{relative}" if normalized_prefix else relative
        if key in existing:
            skipped.append(key)
            continue

        s3.upload_file(str(file_path), bucket, key)
        uploaded.append(key)
        existing.add(key)

    return {
        "bucket": bucket,
        "prefix": normalized_prefix,
        "files_discovered": len(files),
        "files_uploaded": len(uploaded),
        "files_skipped_existing": len(skipped),
        "uploaded_keys": uploaded,
        "skipped_keys": skipped,
    }


def build_s3_client():
    access_key = os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD")
    if not access_key or not secret_key:
        raise RuntimeError(
            "MINIO_ROOT_USER and MINIO_ROOT_PASSWORD must be configured in the environment."
        )

    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", DEFAULT_ENDPOINT),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Incrementally publish ATLAS Parquet files to S3-compatible storage."
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--bucket", default=os.getenv("S3_BUCKET", DEFAULT_BUCKET))
    parser.add_argument("--prefix", default=os.getenv("S3_PREFIX", DEFAULT_PREFIX))
    args = parser.parse_args(argv)

    s3 = build_s3_client()
    ensure_bucket(s3, args.bucket)
    result = sync_parquet_dataset(
        s3,
        source=args.source,
        bucket=args.bucket,
        prefix=args.prefix,
    )
    print("ATLAS S3 DELTA PUBLISH COMPLETE")
    print(f"Bucket: {result['bucket']}")
    print(f"Prefix: {result['prefix']}")
    print(f"Parquet files discovered: {result['files_discovered']}")
    print(f"New files uploaded: {result['files_uploaded']}")
    print(f"Existing files skipped: {result['files_skipped_existing']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
