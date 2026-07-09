import boto3
import io
import os
import pandas as pd
from botocore.config import Config
from rdkit import Chem
from rdkit.Chem import Descriptors, Crippen, Lipinski

BUCKET = "cheminformatics-local"
S3_CONFIG = Config(connect_timeout=3, read_timeout=3, retries={"max_attempts": 1})
S3_ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL")


def _get_s3_client():
    return boto3.client("s3", config=S3_CONFIG, endpoint_url=S3_ENDPOINT_URL)


def calculate_properties(dataset_id: str):
    s3 = _get_s3_client()
    obj = s3.get_object(Bucket=BUCKET, Key=f"generated/{dataset_id}_molecules.csv")
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))

    records = []
    for smi in df["smiles"]:
        mol = Chem.MolFromSmiles(smi)
        if mol is None:
            continue  # Safe-skip invalid chemical strings

        records.append({
            "smiles": smi,
            "molecular_weight": Descriptors.MolWt(mol),
            "logp": Crippen.MolLogP(mol),
            "hba": Lipinski.NumHAcceptors(mol),
            "hbd": Lipinski.NumHDonors(mol),
            "tpsa": Descriptors.TPSA(mol),
        })

    out_df = pd.DataFrame(records)
    buf = io.StringIO()
    out_df.to_csv(buf, index=False)
    s3.put_object(
        Bucket=BUCKET,
        Key=f"properties/{dataset_id}_properties.csv",
        Body=buf.getvalue()
    )
