import boto3
import io
import pandas as pd
import numpy as np
from botocore.config import Config
from sklearn.cluster import KMeans
from rdkit import Chem
from rdkit.Chem import AllChem
import os

S3_ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL")

def _get_s3_client():
    return boto3.client("s3", config=S3_CONFIG, endpoint_url=S3_ENDPOINT_URL)

BUCKET = "cheminformatics-local"
S3_CONFIG = Config(connect_timeout=3, read_timeout=3, retries={"max_attempts": 1})

def cluster_molecules(dataset_id: str, n_clusters: int = 5):
    s3 = boto3.client("s3", config=S3_CONFIG)
    obj = s3.get_object(Bucket=BUCKET, Key=f"properties/{dataset_id}_properties.csv")
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))

    if df.empty:
        raise ValueError(f"No properties found for {dataset_id}")

    fps = []
    valid_indices = []

    for idx, smi in enumerate(df["smiles"]):
        mol = Chem.MolFromSmiles(smi)
        if mol is None:
            continue
        try:
            # Generate 1024-bit Morgan Fingerprints (radius=2)
            fp = AllChem.GetMorganFingerprintAsBitVect(mol, 2, nBits=1024)
            fps.append(list(fp))
            valid_indices.append(idx)
        except Exception:
            continue

    if not fps:
        raise ValueError(f"Failed to generate chemical fingerprints for dataset: {dataset_id}")

    # Sub-select rows that successfully calculated fingerprints
    sub_df = df.iloc[valid_indices].copy()
    X = np.array(fps)

    # Perform K-Means Clustering
    n_clus = min(n_clusters, len(fps))
    kmeans = KMeans(n_clusters=n_clus, random_state=42, n_init=10)
    sub_df["cluster"] = kmeans.fit_predict(X)

    buf = io.StringIO()
    sub_df.to_csv(buf, index=False)
    s3.put_object(
        Bucket=BUCKET,
        Key=f"clustered/{dataset_id}_clustered.csv",
        Body=buf.getvalue()
    )
