import logging

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from .config import S3Endpoint

log = logging.getLogger(__name__)


def client(endpoint: S3Endpoint):
    return boto3.client(
        "s3",
        endpoint_url=endpoint.endpoint_url,
        aws_access_key_id=endpoint.access_key_id,
        aws_secret_access_key=endpoint.secret_access_key,
        region_name=endpoint.region,
        config=BotoConfig(s3={"addressing_style": endpoint.addressing_style}),
    )


def download_bytes(s3, bucket: str, key: str):
    """Returns the object body, or None if it doesn't exist yet (first run)."""
    try:
        return s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise


def upload_bytes(s3, bucket: str, key: str, data: bytes, content_type: str):
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
    log.info("uploaded %d bytes to s3://%s/%s", len(data), bucket, key)
