import io
import os
import boto3
import numpy as np
import pandas as pd
from rdkit import Chem
from rdkit.Chem import AllChem
from sklearn.decomposition import PCA
from faerun import Faerun

BUCKET = "cheminformatics-local"
S3_ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL")


def _get_s3_client():
    return boto3.client("s3", endpoint_url=S3_ENDPOINT_URL)


def build_faerun_graph(dataset_id: str) -> None:
    """Builds an interactive 2D scatter plot of the clustered molecules.

    Uses PCA over Morgan fingerprints for the 2D layout instead of tmap,
    since tmap is a fragile native dependency that isn't worth the install
    risk for an optional feature.
    """
    s3 = _get_s3_client()
    obj = s3.get_object(Bucket=BUCKET, Key=f"clustered/{dataset_id}_clustered.csv")
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))

    if df.empty or len(df) < 2:
        print(f"[faerun] Skipping {dataset_id}: not enough data for a 2D projection")
        return

    fps = []
    valid_rows = []
    for idx, smi in enumerate(df["smiles"]):
        mol = Chem.MolFromSmiles(smi)
        if mol is None:
            continue
        fp = AllChem.GetMorganFingerprintAsBitVect(mol, 2, nBits=1024)
        fps.append(list(fp))
        valid_rows.append(idx)

    if len(fps) < 2:
        print(f"[faerun] Skipping {dataset_id}: fewer than 2 valid fingerprints")
        return

    X = np.array(fps)
    coords = PCA(n_components=2, random_state=42).fit_transform(X)
    sub_df = df.iloc[valid_rows].reset_index(drop=True)

    faerun = Faerun(view="front", coords=False)
    faerun.add_scatter(
        "clusters",
        {
            "x": coords[:, 0].tolist(),
            "y": coords[:, 1].tolist(),
            "c": sub_df["cluster"].tolist(),
            "labels": sub_df["smiles"].tolist(),
        },
        colormap="tab10",
        point_scale=5,
        has_legend=True,
    )

    import tempfile
    import zipfile

    with tempfile.TemporaryDirectory() as tmpdir:
        out_name = f"{dataset_id}_faerun"
        faerun.plot(out_name, path=tmpdir, template="default")

        zip_path = os.path.join(tmpdir, f"{out_name}.zip")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for root, _, files in os.walk(tmpdir):
                for f in files:
                    if f.endswith(".zip"):
                        continue
                    full_path = os.path.join(root, f)
                    zf.write(full_path, arcname=f)

        with open(zip_path, "rb") as f:
            s3.put_object(
                Bucket=BUCKET,
                Key=f"faerun/{dataset_id}_faerun.zip",
                Body=f.read(),
            )

    print(f"[faerun] Wrote faerun graph zip for {dataset_id}")
