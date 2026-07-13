from unittest.mock import patch, MagicMock
import pandas as pd
import io


def _csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


@patch("chem_utils.chemprop_predict.subprocess.run")
@patch("chem_utils.chemprop_predict._get_s3_client")
def test_predict_properties_skips_empty_dataframe(mock_get_s3_client, mock_subprocess):
    from chem_utils.chemprop_predict import predict_properties

    empty_df = pd.DataFrame({"smiles": [], "logp": []})
    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3
    mock_s3.get_object.return_value = {"Body": io.BytesIO(_csv_bytes(empty_df))}

    predict_properties("test001")

    mock_subprocess.assert_not_called()
    mock_s3.put_object.assert_not_called()


@patch("chem_utils.faerun_build._get_s3_client")
def test_build_faerun_graph_skips_insufficient_data(mock_get_s3_client):
    from chem_utils.faerun_build import build_faerun_graph

    tiny_df = pd.DataFrame({"smiles": ["CCO"], "cluster": [0]})
    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3
    mock_s3.get_object.return_value = {"Body": io.BytesIO(_csv_bytes(tiny_df))}

    build_faerun_graph("test002")

    mock_s3.put_object.assert_not_called()
