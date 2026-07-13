import io
import os
import subprocess
import tempfile
import boto3
import pandas as pd

BUCKET = "cheminformatics-local"
S3_ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL")


def _get_s3_client():
    return boto3.client("s3", endpoint_url=S3_ENDPOINT_URL)


def predict_properties(dataset_id: str) -> None:
    """Demonstration-only ChemProp integration.

    ChemProp is normally used to predict properties that can't be computed
    exactly (e.g. measured activity, solubility). This dataset has no such
    labels, so this trains a toy regression model using our own RDKit-computed
    logP as a pseudo-label, purely to prove the integration works end-to-end.
    Real usage would replace `logp` with a genuine experimental target column.
    """
    s3 = _get_s3_client()
    obj = s3.get_object(Bucket=BUCKET, Key=f"clustered/{dataset_id}_clustered.csv")
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))

    if df.empty or "logp" not in df.columns:
        print(f"[chemprop] Skipping {dataset_id}: no data or missing logp column")
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        train_path = os.path.join(tmpdir, "train.csv")
        model_dir = os.path.join(tmpdir, "model")
        preds_path = os.path.join(tmpdir, "preds.csv")

        df[["smiles", "logp"]].to_csv(train_path, index=False)

        train_cmd = [
            "chemprop_train",
            "--data_path", train_path,
            "--dataset_type", "regression",
            "--save_dir", model_dir,
            "--epochs", "3",
            "--quiet",
        ]
        try:
            result = subprocess.run(train_cmd, capture_output=True, text=True, timeout=120)
        except subprocess.TimeoutExpired:
            print(f"[chemprop] Training timed out for {dataset_id}, skipping")
            return

        if result.returncode != 0:
            print(f"[chemprop] Training failed for {dataset_id}: {result.stderr[-1000:]}")
            return

        predict_cmd = [
            "chemprop_predict",
            "--test_path", train_path,
            "--checkpoint_dir", model_dir,
            "--preds_path", preds_path,
        ]
        try:
            result = subprocess.run(predict_cmd, capture_output=True, text=True, timeout=60)
        except subprocess.TimeoutExpired:
            print(f"[chemprop] Prediction timed out for {dataset_id}, skipping")
            return

        if result.returncode != 0:
            print(f"[chemprop] Prediction failed for {dataset_id}: {result.stderr[-1000:]}")
            return

        preds_df = pd.read_csv(preds_path)

    buf = io.StringIO()
    preds_df.to_csv(buf, index=False)
    s3.put_object(
        Bucket=BUCKET,
        Key=f"predictions/{dataset_id}_predictions.csv",
        Body=buf.getvalue(),
    )
    print(f"[chemprop] Wrote predictions for {dataset_id}")
