import boto3
import os

BUCKET = "cheminformatics-local"
S3_ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL")


def _get_s3_client():
    return boto3.client("s3", endpoint_url=S3_ENDPOINT_URL)


def discover_new_datasets(overwrite: bool, prev_success=None):
    """List scaffold files in S3, filter to only new ones since last successful
    run unless overwrite=True."""
    s3 = _get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    dataset_ids = set()

    for page in paginator.paginate(Bucket=BUCKET):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("_scaffolds.csv"):
                continue
            if not overwrite and prev_success and obj["LastModified"] <= prev_success:
                continue
            dataset_id = key.rsplit("/", 1)[-1].replace("_scaffolds.csv", "")
            dataset_ids.add(dataset_id)

    return list(dataset_ids)
