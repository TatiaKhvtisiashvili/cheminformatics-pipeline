from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
from chem_utils.discovery import discover_new_datasets


@patch("chem_utils.discovery._get_s3_client")
def test_discover_returns_all_when_no_prev_run(mock_get_s3_client):
    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3
    mock_paginator = MagicMock()
    mock_s3.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [{
        "Contents": [
            {"Key": "batch001_scaffolds.csv", "LastModified": datetime(2026, 1, 1, tzinfo=timezone.utc)},
            {"Key": "batch002_scaffolds.csv", "LastModified": datetime(2026, 1, 2, tzinfo=timezone.utc)},
            {"Key": "batch001_r_groups.csv", "LastModified": datetime(2026, 1, 1, tzinfo=timezone.utc)},
        ]
    }]

    result = discover_new_datasets(overwrite=False, prev_success=None)

    assert set(result) == {"batch001", "batch002"}


@patch("chem_utils.discovery._get_s3_client")
def test_discover_skips_old_files_without_overwrite(mock_get_s3_client):
    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3
    mock_paginator = MagicMock()
    mock_s3.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [{
        "Contents": [
            {"Key": "batch001_scaffolds.csv", "LastModified": datetime(2026, 1, 1, tzinfo=timezone.utc)},
            {"Key": "batch002_scaffolds.csv", "LastModified": datetime(2026, 1, 5, tzinfo=timezone.utc)},
        ]
    }]

    prev_success = datetime(2026, 1, 3, tzinfo=timezone.utc)
    result = discover_new_datasets(overwrite=False, prev_success=prev_success)

    assert result == ["batch002"]


@patch("chem_utils.discovery._get_s3_client")
def test_discover_returns_all_with_overwrite_true(mock_get_s3_client):
    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3
    mock_paginator = MagicMock()
    mock_s3.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [{
        "Contents": [
            {"Key": "batch001_scaffolds.csv", "LastModified": datetime(2026, 1, 1, tzinfo=timezone.utc)},
        ]
    }]

    prev_success = datetime(2026, 1, 3, tzinfo=timezone.utc)
    result = discover_new_datasets(overwrite=True, prev_success=prev_success)

    assert result == ["batch001"]
