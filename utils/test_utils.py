"""
Test cases for functions in utils.py.
Run with: pytest utils/test_utils.py -v
"""

import json
import os
import tempfile
from collections import defaultdict
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from utils.utils import (
    PAN_KEY,
    NAME_KEY,
    load_secrets,
    neo4j_connect,
    athena_connect,
    retrieve_api_logs,
    subgraph_query,
    retrive_subgraphs,
    is_seed_node,
    write_subgraphs,
    hash_identity,
    extract_ids_from_logs,
    pan_profile_processing,
    dl_processing,
    phone_processing,
    email_processing,
    rc_processing,
)


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


# --- hash_identity ---


def test_hash_identity_masks_value():
    """hash_identity replaces value with masked_<sha256_prefix>."""
    node = {"labels": ["email"], "properties": {"value": "user@example.com"}}
    out = hash_identity(node)
    assert out["labels"] == ["email"]
    assert out["properties"]["value"].startswith("masked_")
    assert len(out["properties"]["value"]) == len("masked_") + 10


def test_hash_identity_preserves_relation_count_and_last_seen():
    """hash_identity retains relation_count and last_seen from properties."""
    node = {
        "labels": ["phone"],
        "properties": {"value": "123", "relation_count": 5, "last_seen": "2025-01-01"},
    }
    out = hash_identity(node)
    assert out["relation_count"] == 5
    assert out["last_seen"] == "2025-01-01"


def test_hash_identity_no_value_in_properties():
    """hash_identity handles properties without value key."""
    node = {"labels": ["email"], "properties": {"other": "x"}}
    out = hash_identity(node)
    assert out["labels"] == ["email"]
    assert out["properties"] == {"other": "x"}


def test_hash_identity_empty_node():
    """hash_identity returns empty dict for node with no labels or properties."""
    node = {}
    out = hash_identity(node)
    assert out == {}


# --- is_seed_node ---


def test_is_seed_node_exact_match_with_seed_sets():
    """is_seed_node returns True when node matches this_seed_type and this_seed_value and seed_identity_sets is non-empty."""
    node = {"labels": ["email"], "properties": {"value": "a@b.com"}}
    assert is_seed_node(node, "email", "a@b.com", {"email": {"a@b.com"}}) is True


def test_is_seed_node_value_in_seed_identity_sets():
    """is_seed_node returns True when node value is in any seed identity set."""
    node = {"labels": ["phone"], "properties": {"value": "999"}}
    seed_sets = {"phone": {"999"}, "email": set()}
    assert is_seed_node(node, "email", "other@x.com", seed_sets) is True


def test_is_seed_node_no_match_with_seed_sets():
    """is_seed_node returns False when value not in seed sets and no exact match."""
    node = {"labels": ["email"], "properties": {"value": "other@x.com"}}
    seed_sets = {"email": {"a@b.com"}}
    assert is_seed_node(node, "email", "a@b.com", seed_sets) is False


def test_is_seed_node_no_seed_sets_exact_match():
    """is_seed_node returns True when seed_identity_sets is empty/falsy and node matches this_seed_type/value."""
    node = {"labels": ["email"], "properties": {"value": "root@x.com"}}
    assert is_seed_node(node, "email", "root@x.com", None) is True
    assert is_seed_node(node, "email", "root@x.com", {}) is True


def test_is_seed_node_no_seed_sets_no_match():
    """is_seed_node returns False when seed_identity_sets is empty and node does not match."""
    node = {"labels": ["email"], "properties": {"value": "other@x.com"}}
    assert is_seed_node(node, "email", "root@x.com", None) is False


def test_is_seed_node_missing_labels_or_properties():
    """is_seed_node returns False when node lacks labels or value."""
    assert is_seed_node({}, "email", "a@b.com", None) is False
    assert is_seed_node({"labels": ["email"]}, "email", "a@b.com", None) is False
    assert (
        is_seed_node({"properties": {"value": "a@b.com"}}, "email", "a@b.com", None)
        is False
    )


# --- pan_profile_processing ---


def test_pan_profile_processing():
    """pan_profile_processing extracts pan from request and name from response."""
    logs = pd.DataFrame(
        {
            "merchantrequestbody": ['{"pan": "ABCDE1234F"}', '{"pan": "XYZ"}'],
            "response": ['{"name": "Alice"}', '{"name": "Bob"}'],
        }
    )
    result = pan_profile_processing(logs)
    assert result[PAN_KEY] == ["ABCDE1234F", "XYZ"]
    assert result[NAME_KEY] == ["Alice", "Bob"]


def test_pan_profile_processing_missing_keys():
    """pan_profile_processing handles missing pan/name in JSON."""
    logs = pd.DataFrame(
        {
            "merchantrequestbody": ["{}"],
            "response": ["{}"],
        }
    )
    result = pan_profile_processing(logs)
    assert result[PAN_KEY] == [None]
    assert result[NAME_KEY] == [None]


# --- extract_ids_from_logs ---


def test_extract_ids_from_logs_pan_profile():
    """extract_ids_from_logs dispatches to pan_profile_processing for pan-profile."""
    logs = pd.DataFrame(
        {
            "merchantrequestbody": ['{"pan": "P1"}'],
            "response": ['{"name": "N1"}'],
        }
    )
    result = extract_ids_from_logs(logs, "pan-profile")
    assert result[PAN_KEY] == ["P1"]
    assert result[NAME_KEY] == ["N1"]


def test_extract_ids_from_logs_unknown_event_type():
    """extract_ids_from_logs raises KeyError for unknown event type."""
    logs = pd.DataFrame({"merchantrequestbody": [], "response": []})
    with pytest.raises(KeyError):
        extract_ids_from_logs(logs, "unknown-event")


# --- retrieve_api_logs ---


def test_retrieve_api_logs_requires_start_or_retention():
    """retrieve_api_logs raises when both start_date and data_retention_days are None."""
    mock_client = MagicMock()
    with pytest.raises(Exception, match="either start date or data retention days"):
        retrieve_api_logs(
            "merchant1",
            "2025-01-15",
            mock_client,
            start_date=None,
            data_retention_days=None,
        )


def test_retrieve_api_logs_with_start_date():
    """retrieve_api_logs runs query when start_date is provided."""
    mock_client = MagicMock()
    mock_client.query.return_value.to_pandas.return_value = pd.DataFrame()
    result = retrieve_api_logs(
        "merchant1", "2025-01-15", mock_client, start_date="2025-01-01"
    )
    assert mock_client.query.called
    assert isinstance(result, pd.DataFrame)


# --- retrive_subgraphs ---


@patch("utils.utils.subgraph_query")
def test_retrive_subgraphs_structure(mock_subgraph_query):
    """retrive_subgraphs returns dict of id_type -> list of subgraphs."""
    mock_subgraph_query.return_value = [
        {
            "seed_type": "email",
            "seed_value": "a@b.com",
            "nodes": [],
            "relationships": [],
        }
    ]
    seed_identities = {"email": ["a@b.com"], "phone": ["123"]}
    result = retrive_subgraphs(seed_identities, max_depth=2)
    assert "email" in result
    assert "phone" in result
    assert len(result["email"]) == 1
    assert len(result["phone"]) == 1
    assert result["email"][0]["seed_value"] == "a@b.com"


# --- neo4j_connect / athena_connect ---


@patch("utils.utils.load_secrets")
@patch("utils.utils.GraphDatabase")
def test_neo4j_connect_uses_secrets(mock_graph_db, mock_load_secrets):
    """neo4j_connect loads secrets and passes neo4j config to driver."""
    mock_load_secrets.return_value = {
        "neo4j": {"uri": "bolt://localhost", "auth": ("u", "p")}
    }
    mock_driver = MagicMock()
    mock_graph_db.driver.return_value = mock_driver
    out = neo4j_connect(secrets_file="secrets.yml")
    mock_load_secrets.assert_called_once_with("secrets.yml")
    mock_graph_db.driver.assert_called_once_with(
        uri="bolt://localhost", auth=("u", "p")
    )
    assert out is mock_driver


@patch("utils.utils.load_secrets")
@patch("utils.utils.connect")
def test_athena_connect_uses_secrets(mock_connect, mock_load_secrets):
    """athena_connect loads secrets and passes athena config to connect."""
    mock_load_secrets.return_value = {"athena": {"region_name": "us-east-1"}}
    mock_client = MagicMock()
    mock_connect.return_value = mock_client
    out = athena_connect(secrets_file="secrets.yml")
    mock_load_secrets.assert_called_once_with("secrets.yml")
    mock_connect.assert_called_once_with(region_name="us-east-1")
    assert out is mock_client


# --- write_subgraphs ---


@patch("utils.utils.neo4j_connect")
def test_write_subgraphs_merges_nodes_and_relationships(mock_neo4j_connect):
    """write_subgraphs calls session.run for nodes and relationships."""
    mock_session = MagicMock()
    mock_driver = MagicMock()
    mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_driver.session.return_value.__exit__ = MagicMock(return_value=None)
    mock_neo4j_connect.return_value = mock_driver

    subgraphs = [
        {
            "seed_type": "email",
            "seed_value": "a@b.com",
            "nodes": [
                {"id": 1, "labels": ["email"], "properties": {"value": "a@b.com"}},
                {"id": 2, "labels": ["phone"], "properties": {"value": "999"}},
            ],
            "relationships": [
                {"start": 1, "end": 2, "type": "LINKED_TO", "properties": {}}
            ],
        }
    ]
    write_subgraphs(
        subgraphs, secrets_file="secrets.yml", seed_identities={"email": {"a@b.com"}}
    )
    assert mock_session.run.call_count >= 1


# --- Stub processors (pass) ---


def test_dl_processing_returns_none():
    """dl_processing is a stub and returns None."""
    logs = pd.DataFrame()
    assert dl_processing(logs) is None


def test_phone_processing_returns_none():
    """phone_processing is a stub and returns None."""
    logs = pd.DataFrame()
    assert phone_processing(logs) is None


def test_email_processing_returns_none():
    """email_processing is a stub and returns None."""
    logs = pd.DataFrame()
    assert email_processing(logs) is None


def test_rc_processing_returns_none():
    """rc_processing is a stub and returns None."""
    logs = pd.DataFrame()
    assert rc_processing(logs) is None
