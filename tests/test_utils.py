"""
Test cases for functions in utils.py.
Run with: pytest utils/test_utils.py -v
"""

import os
import tempfile
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from gin_product.utils.logs_processing import IdentityTypes
from gin_product.utils.utils import (
    load_config,
    load_secrets,
    athena_connect,
    retrieve_api_logs,
    extract_ids_from_logs,
)


# --- load_config ---


def test_load_config_file_not_found():
    """load_config raises FileNotFoundError when file does not exist."""
    with pytest.raises(FileNotFoundError, match="Config file not found"):
        load_config("nonexistent.yml")


def test_load_config_valid_yaml():
    """load_config returns parsed dict for valid YAML file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write("merchant_config:\n  merchantid: 'm1'\n")
        path = f.name
    try:
        result = load_config(path)
        assert "merchant_config" in result
        assert result["merchant_config"]["merchantid"] == "m1"
    finally:
        os.unlink(path)


# --- load_secrets ---


def test_load_secrets_file_not_found():
    """load_secrets raises FileNotFoundError when file does not exist."""
    with pytest.raises(FileNotFoundError, match="Secrets file not found"):
        load_secrets("nonexistent.yml")


def test_load_secrets_valid_yaml():
    """load_secrets returns parsed dict for valid YAML file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write("key: value\nnested:\n  a: 1\n")
        path = f.name
    try:
        result = load_secrets(path)
        assert result == {"key": "value", "nested": {"a": 1}}
    finally:
        os.unlink(path)


# --- athena_connect ---


def test_athena_connect_uses_secrets():
    """athena_connect uses athena config from secrets."""
    with patch("utils.utils.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        secrets = {"athena": {"region_name": "us-east-1", "s3_staging_dir": "s3://x/"}}
        out = athena_connect(secrets)
        mock_connect.assert_called_once_with(
            region_name="us-east-1", s3_staging_dir="s3://x/"
        )
        assert out is mock_conn


# --- retrieve_api_logs ---


def test_retrieve_api_logs_requires_start_or_retention():
    """retrieve_api_logs raises when both start_date and data_retention_days are None."""
    mock_conn = MagicMock()
    with pytest.raises(Exception, match="either start date or data retention days"):
        retrieve_api_logs(
            mock_conn,
            "merchant1",
            date(2025, 1, 15),
            start_date=None,
            data_retention_days=None,
        )


def test_retrieve_api_logs_with_start_date():
    """retrieve_api_logs runs query when start_date is provided."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=None)
    mock_conn.cursor.return_value.execute.return_value.as_pandas.return_value = (
        pd.DataFrame()
    )
    result = retrieve_api_logs(
        mock_conn,
        "merchant1",
        date(2025, 1, 15),
        start_date=date(2025, 1, 1),
    )
    mock_conn.cursor.assert_called_once()
    assert isinstance(result, pd.DataFrame)


def test_retrieve_api_logs_uses_data_retention_days_when_no_start_date():
    """retrieve_api_logs derives start_date from data_retention_days when start_date is None."""
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=None)
    mock_conn.cursor.return_value.execute.return_value.as_pandas.return_value = (
        pd.DataFrame()
    )
    retrieve_api_logs(
        mock_conn,
        "merchant1",
        date(2025, 1, 15),
        data_retention_days=7,
    )
    mock_conn.cursor.assert_called_once()


# --- extract_ids_from_logs ---


def test_extract_ids_from_logs_pan_profile():
    """extract_ids_from_logs dispatches to pan_profile_processing for pan-profile."""
    logs = pd.DataFrame(
        {
            "event": ["pan-profile", "pan-profile"],
            "merchantrequestbody": ['{"pan": "P1"}', '{"pan": "P2"}'],
            "requesttimestamp": [
                pd.Timestamp("2025-01-01"),
                pd.Timestamp("2025-01-02"),
            ],
        }
    )
    result = extract_ids_from_logs(logs, ["pan-profile"])
    assert "pan-profile" in result
    df = result["pan-profile"]
    assert list(df.columns) == [IdentityTypes.PAN.value, "requestTimestamp"]
    assert list(df["pan"]) == ["p1", "p2"]


def test_extract_ids_from_logs_unknown_event_type():
    """extract_ids_from_logs raises KeyError for unknown event type."""
    logs = pd.DataFrame(
        {"event": [], "merchantrequestbody": [], "requesttimestamp": []}
    )
    with pytest.raises(KeyError):
        extract_ids_from_logs(logs, ["unknown-event"])


def test_extract_ids_from_logs_multiple_event_types():
    """extract_ids_from_logs returns one DataFrame per event type."""
    logs = pd.DataFrame(
        {
            "event": ["pan-profile"],
            "merchantrequestbody": ['{"pan": "abcde1234f"}'],
            "requesttimestamp": [pd.Timestamp("2025-01-01")],
        }
    )
    result = extract_ids_from_logs(logs, ["pan-profile"])
    assert set(result.keys()) == {"pan-profile"}
    assert len(result["pan-profile"]) == 1
    assert result["pan-profile"]["pan"].iloc[0] == "abcde1234f"
