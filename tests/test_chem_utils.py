import io
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from chem_utils.molecule_gen import generate_molecules
from chem_utils.properties import calculate_properties
from chem_utils.clustering import cluster_molecules


def _csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


@patch("chem_utils.molecule_gen._get_s3_client")
def test_generate_molecules_combines_scaffold_and_r_group(mock_get_s3_client):
    scaffolds_df = pd.DataFrame({"smiles": ["CCC*"]})
    r_groups_df = pd.DataFrame({"smiles": ["*Cl"]})

    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3

    def get_object_side_effect(Bucket, Key):
        if "scaffolds" in Key:
            return {"Body": io.BytesIO(_csv_bytes(scaffolds_df))}
        if "r_groups" in Key:
            return {"Body": io.BytesIO(_csv_bytes(r_groups_df))}
        raise ValueError(f"Unexpected key: {Key}")

    mock_s3.get_object.side_effect = get_object_side_effect

    generate_molecules("test001")

    assert mock_s3.put_object.called
    call_kwargs = mock_s3.put_object.call_args.kwargs
    assert call_kwargs["Key"] == "generated/test001_molecules.csv"
    written_df = pd.read_csv(io.BytesIO(call_kwargs["Body"].encode("utf-8")))
    assert "smiles" in written_df.columns
    assert len(written_df) > 0


@patch("chem_utils.molecule_gen._get_s3_client")
def test_generate_molecules_skips_invalid_smiles(mock_get_s3_client):
    scaffolds_df = pd.DataFrame({"smiles": ["not_a_real_smiles"]})
    r_groups_df = pd.DataFrame({"smiles": ["*Cl"]})

    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3

    def get_object_side_effect(Bucket, Key):
        if "scaffolds" in Key:
            return {"Body": io.BytesIO(_csv_bytes(scaffolds_df))}
        return {"Body": io.BytesIO(_csv_bytes(r_groups_df))}

    mock_s3.get_object.side_effect = get_object_side_effect

    generate_molecules("test002")

    call_kwargs = mock_s3.put_object.call_args.kwargs
    written_df = pd.read_csv(io.BytesIO(call_kwargs["Body"].encode("utf-8")))
    assert len(written_df) == 0  # invalid scaffold should be skipped, nothing generated


@patch("chem_utils.properties._get_s3_client")
def test_calculate_properties_outputs_expected_columns(mock_get_s3_client):
    molecules_df = pd.DataFrame({"smiles": ["CCO", "c1ccccc1"]})

    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3
    mock_s3.get_object.return_value = {"Body": io.BytesIO(_csv_bytes(molecules_df))}

    calculate_properties("test003")

    call_kwargs = mock_s3.put_object.call_args.kwargs
    assert call_kwargs["Key"] == "properties/test003_properties.csv"
    written_df = pd.read_csv(io.BytesIO(call_kwargs["Body"].encode("utf-8")))
    for col in ["molecular_weight", "logp", "hba", "hbd", "tpsa"]:
        assert col in written_df.columns
    assert len(written_df) == 2


@patch("chem_utils.clustering._get_s3_client")
def test_cluster_molecules_assigns_cluster_column(mock_get_s3_client):
    properties_df = pd.DataFrame({
        "smiles": ["CCO", "c1ccccc1", "CCC", "CCN", "CCCl"],
        "molecular_weight": [46.07, 78.11, 44.1, 45.08, 64.51],
        "logp": [-0.14, 2.13, 1.09, -0.27, 0.7],
    })

    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3
    mock_s3.get_object.return_value = {"Body": io.BytesIO(_csv_bytes(properties_df))}

    cluster_molecules("test004", n_clusters=2)

    call_kwargs = mock_s3.put_object.call_args.kwargs
    assert call_kwargs["Key"] == "clustered/test004_clustered.csv"
    written_df = pd.read_csv(io.BytesIO(call_kwargs["Body"].encode("utf-8")))
    assert "cluster" in written_df.columns
    assert written_df["cluster"].nunique() <= 2


@patch("chem_utils.clustering._get_s3_client")
def test_cluster_molecules_raises_on_empty_properties(mock_get_s3_client):
    empty_df = pd.DataFrame({"smiles": [], "molecular_weight": [], "logp": []})

    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3
    mock_s3.get_object.return_value = {"Body": io.BytesIO(_csv_bytes(empty_df))}

    with pytest.raises(ValueError):
        cluster_molecules("test005")
