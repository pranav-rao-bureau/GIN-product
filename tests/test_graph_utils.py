"""
Test cases for graph-related functions and classes in graph_utils.py.
Run with: pytest utils/test_graph_utils.py -v
"""

import hashlib
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from gin_product.utils.graph_utils import SubgraphWriter


# --- SubgraphWriter.hash_identity ---


def test_hash_identity_adds_hashed_value():
    """hash_identity adds hashed_value (SHA256 hex) to properties, keeps value."""
    node = {"labels": ["email"], "properties": {"value": "user@example.com"}}
    out = SubgraphWriter.hash_identity(node, is_seed_node=False)
    assert out["labels"] == ["email"]
    assert out["properties"]["value"] == "user@example.com"
    expected_hash = hashlib.sha256(b"user@example.com").hexdigest()
    assert out["properties"]["hashed_value"] == expected_hash


def test_hash_identity_preserves_other_properties():
    """hash_identity retains other properties when adding hashed_value."""
    node = {
        "labels": ["phone"],
        "properties": {"value": "123", "relation_count": 5, "last_seen": "2025-01-01"},
    }
    out = SubgraphWriter.hash_identity(node, is_seed_node=True)
    assert out["properties"]["value"] == "123"
    assert out["properties"]["relation_count"] == 5
    assert out["properties"]["last_seen"] == "2025-01-01"
    assert "hashed_value" in out["properties"]


def test_hash_identity_no_value_in_properties():
    """hash_identity leaves properties unchanged when value key is absent."""
    node = {"labels": ["email"], "properties": {"other": "x"}}
    out = SubgraphWriter.hash_identity(node, is_seed_node=False)
    assert out["labels"] == ["email"]
    assert out["properties"] == {"other": "x"}


def test_hash_identity_empty_node():
    """hash_identity returns copy of node with no properties when node is empty."""
    node = {}
    out = SubgraphWriter.hash_identity(node, is_seed_node=False)
    assert out == {}


def test_hash_identity_does_not_mutate_input():
    """hash_identity returns a copy; input node is unchanged."""
    node = {"labels": ["pan"], "properties": {"value": "abc"}}
    out = SubgraphWriter.hash_identity(node, is_seed_node=False)
    assert "hashed_value" not in node["properties"]
    assert "hashed_value" in out["properties"]


# --- SubgraphWriter.is_seed_node ---


def test_is_seed_node_true_when_value_in_seed_set():
    """is_seed_node returns True when node type and value are in seed_identities."""
    node = {"labels": ["email"], "properties": {"value": "a@b.com"}}
    seed_identities = {"email": {"a@b.com"}, "phone": set()}
    assert SubgraphWriter.is_seed_node(node, seed_identities) is True


def test_is_seed_node_true_for_other_type_in_seed_set():
    """is_seed_node returns True when node value is in any seed identity set."""
    node = {"labels": ["phone"], "properties": {"value": "999"}}
    seed_identities = {"phone": {"999"}, "email": set()}
    assert SubgraphWriter.is_seed_node(node, seed_identities) is True


def test_is_seed_node_false_when_value_not_in_seed_set():
    """is_seed_node returns False when value not in seed set for that type."""
    node = {"labels": ["email"], "properties": {"value": "other@x.com"}}
    seed_identities = {"email": {"a@b.com"}}
    assert SubgraphWriter.is_seed_node(node, seed_identities) is False


def test_is_seed_node_false_when_empty_seed_identities():
    """is_seed_node returns False when seed_identities is empty."""
    node = {"labels": ["email"], "properties": {"value": "a@b.com"}}
    assert SubgraphWriter.is_seed_node(node, {}) is False


def test_is_seed_node_false_missing_labels():
    """is_seed_node returns False when node has no labels."""
    node = {"properties": {"value": "a@b.com"}}
    seed_identities = {"email": {"a@b.com"}}
    assert SubgraphWriter.is_seed_node(node, seed_identities) is False


def test_is_seed_node_false_missing_value():
    """is_seed_node returns False when node has no value in properties."""
    node = {"labels": ["email"], "properties": {}}
    seed_identities = {"email": {"a@b.com"}}
    assert SubgraphWriter.is_seed_node(node, seed_identities) is False


def test_is_seed_node_false_label_not_in_seed_identities():
    """is_seed_node returns False when node label is not a key in seed_identities."""
    node = {"labels": ["pan"], "properties": {"value": "abc"}}
    seed_identities = {"email": {"a@b.com"}}
    assert SubgraphWriter.is_seed_node(node, seed_identities) is False
