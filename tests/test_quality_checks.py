import io
from unittest.mock import patch, MagicMock
import pandas as pd
from chem_utils.quality_checks import run_quality_checks


def _csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


@patch("chem_utils.quality_checks._get_s3_client")
def test_run_quality_checks_passes_clean_data(mock_get_s3_client):
    df = pd.DataFrame({
        "smiles": ["CCO", "c1ccccc1"],
        "molecular_weight": [46.07, 78.11],
        "logp": [-0.14, 2.13],
    })
    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3
    mock_s3.get_object.return_value = {"Body": io.BytesIO(_csv_bytes(df))}

    issues = run_quality_checks("test001")

    assert issues == []


@patch("chem_utils.quality_checks._get_s3_client")
def test_run_quality_checks_flags_empty_dataframe(mock_get_s3_client):
    df = pd.DataFrame({"smiles": [], "molecular_weight": [], "logp": []})
    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3
    mock_s3.get_object.return_value = {"Body": io.BytesIO(_csv_bytes(df))}

    issues = run_quality_checks("test002")

    assert "No molecules generated for dataset" in issues


@patch("chem_utils.quality_checks._get_s3_client")
def test_run_quality_checks_flags_non_positive_molecular_weight(mock_get_s3_client):
    df = pd.DataFrame({
        "smiles": ["CCO"],
        "molecular_weight": [-5.0],
        "logp": [1.0],
    })
    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3
    mock_s3.get_object.return_value = {"Body": io.BytesIO(_csv_bytes(df))}

    issues = run_quality_checks("test003")

    assert any("Non-positive molecular_weight" in issue for issue in issues)


@patch("chem_utils.quality_checks._get_s3_client")
def test_run_quality_checks_flags_null_logp(mock_get_s3_client):
    df = pd.DataFrame({
        "smiles": ["CCO"],
        "molecular_weight": [46.07],
        "logp": [None],
    })
    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3
    mock_s3.get_object.return_value = {"Body": io.BytesIO(_csv_bytes(df))}

    issues = run_quality_checks("test004")

    assert any("Null logp" in issue for issue in issues)
