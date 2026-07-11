import io
import boto3
import os
import pandas as pd

BUCKET = "cheminformatics-local"
S3_ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL")


def _get_s3_client():
    return boto3.client("s3", endpoint_url=S3_ENDPOINT_URL)


def run_quality_checks(dataset_id: str) -> list:
    """Return a list of quality issues found in the properties file.
    Empty list means the dataset passed all checks."""
    s3 = _get_s3_client()
    obj = s3.get_object(Bucket=BUCKET, Key=f"properties/{dataset_id}_properties.csv")
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))

    issues = []

    if df.empty:
        issues.append("No molecules generated for dataset")
        return issues  # nothing further to check on an empty dataframe

    if df["molecular_weight"].isnull().any():
        issues.append("Null molecular_weight values found")
    if (df["molecular_weight"] <= 0).any():
        issues.append("Non-positive molecular_weight values found")
    if df["logp"].isnull().any():
        issues.append("Null logp values found")
    if df["smiles"].isnull().any() or (df["smiles"] == "").any():
        issues.append("Blank SMILES strings found")

    return issues
