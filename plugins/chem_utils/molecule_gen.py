import boto3
import io
import pandas as pd
from botocore.config import Config
from rdkit import Chem

BUCKET = "cheminformatics-input"

# Robust configuration to prevent indefinite hangs in case of network or S3 auth issues
S3_CONFIG = Config(
    connect_timeout=3,
    read_timeout=3,
    retries={"max_attempts": 1}
)


def _read_csv(bucket: str, key: str):
    """Safely fetch CSV from S3 with built-in timeouts."""
    s3 = boto3.client("s3", config=S3_CONFIG)
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def generate_molecules(dataset_id: str):
    """Combines scaffold and R-groups into products using RDKit."""
    scaffolds = _read_csv(BUCKET, f"{dataset_id}_scaffolds.csv")
    r_groups = _read_csv(BUCKET, f"{dataset_id}_r_groups.csv")

    molecules = set()  # Using set to avoid duplicate SMILES
    for scaffold_smiles in scaffolds["smiles"]:
        scaffold_mol = Chem.MolFromSmiles(scaffold_smiles)
        if scaffold_mol is None:
            continue

        for r_smiles in r_groups["smiles"]:
            r_mol = Chem.MolFromSmiles(r_smiles)
            if r_mol is None:
                continue

            # Perform a safe molecule combination
            combined = Chem.CombineMols(scaffold_mol, r_mol)
            if combined is not None:
                try:
                    # Sanitize to ensure valid structure
                    Chem.SanitizeMol(combined)
                    smi = Chem.MolToSmiles(combined)
                    molecules.add(smi)
                except Exception:
                    # Skip molecules that fail sanitization
                    continue

    out_df = pd.DataFrame({"smiles": list(molecules)})

    # Save output to S3
    s3 = boto3.client("s3", config=S3_CONFIG)
    buf = io.StringIO()
    out_df.to_csv(buf, index=False)
    s3.put_object(
        Bucket=BUCKET,
        Key=f"generated/{dataset_id}_molecules.csv",
        Body=buf.getvalue()
    )
