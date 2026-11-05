import os
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
BUCKET = os.getenv("S3_BUCKET", "atlas-lake")
SOURCE = Path("data/processed/events")

def main():
    access_key = os.environ["MINIO_ROOT_USER"]
    secret_key = os.environ["MINIO_ROOT_PASSWORD"]

    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )

    try:
        s3.head_bucket(Bucket=BUCKET)
        print(f"Bucket already exists: {BUCKET}")
    except ClientError as error:
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status not in (404, 400):
            raise
        s3.create_bucket(Bucket=BUCKET)
        print(f"Created bucket: {BUCKET}")

    files = sorted(SOURCE.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No Parquet files found in {SOURCE}")

    uploaded = 0
    for file in files:
        relative = file.relative_to(SOURCE)
        key = f"silver/events/{relative.as_posix()}"
        s3.upload_file(str(file), BUCKET, key)
        print(f"Uploaded: s3://{BUCKET}/{key}")
        uploaded += 1

    print(f"\nATLAS DATA LAKE UPLOAD COMPLETE")
    print(f"Files uploaded: {uploaded}")
    print(f"Bucket: {BUCKET}")

if __name__ == "__main__":
    main()
