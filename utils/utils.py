from collections import defaultdict
import hashlib
import os
from typing import Dict, List

import json
import pandas as pd
from pyathena import connect
import yaml

from neo4j import GraphDatabase


class IdentityTypes:
    PAN = "pan"
    NAME = "fullName"
    DL = "dlNumber"
    RC = "vehicleRC"
    PHONE = "mobile"
    EMAIL = "email"
    VOTER = "voterId"
    ADDRESS = "completeAddress"


def load_secrets(secrets_file: str = "secrets.yml") -> dict:
    """Load secrets from a YAML file."""
    if not os.path.exists(secrets_file):
        raise FileNotFoundError(f"Secrets file not found: {secrets_file}")
    with open(secrets_file, "r") as f:
        secrets = yaml.load(f, Loader=yaml.FullLoader)
    return secrets


def neo4j_connect(secrets, read_or_write: str = "read"):
    """
    Returns a valid neo4j connection object using credentials from secrets file.

    If config is provided, it overrides values in secrets file.
    """
    neo4j_cfg = secrets["{read_or_write}_neo4j"]
    driver = GraphDatabase.driver(**neo4j_cfg)
    return driver


def athena_connect(secrets):
    """
    Uses boto3 and a config/secrets_file to create an Athena client object.
    """
    athena_cfg = secrets["athena"]
    athena_client = connect(**athena_cfg)
    return athena_client


def retrieve_api_logs(
    athena_client,
    merchant_id: str,
    end_date: str,
    start_date: str = None,
    data_retention_days: int = None,
):
    """
    Query all identities for a merchant_id for a given time frame from the API call logs.
    """

    if start_date is None and data_retention_days is None:
        raise Exception("Please provide either start date or data retention days")

    if start_date is None:
        start_date = end_date - data_retention_days

    query = f"""
    SELECT merchantid,
        event, 
        merchantrequestbody,
        response,
        requesttimestamp,
    FROM base.service_api_logevent_pqt
    where merchantid = {merchant_id}
    and requesttimestamp between {start_date} and {end_date}
    and merchantstatuscode = 200
    """

    return athena_client.query(query).to_pandas()


def is_seed_node(node, this_seed_type, this_seed_value, seed_identity_sets):
    # If explicit seed_identities info is present, check for a seed node
    if seed_identity_sets:
        # The root node for this subgraph must always be unmasked
        if (
            "labels" in node
            and this_seed_type in node["labels"]
            and "properties" in node
            and "value" in node["properties"]
            and node["properties"]["value"] == this_seed_value
        ):
            return True
        # Otherwise check if node's value is in any seed identity set
        for typ, seedvals in seed_identity_sets.items():
            if "labels" in node and typ in node["labels"]:
                value = node.get("properties", {}).get("value", None)
                if value and value in seedvals:
                    return True
    else:
        # If no seed_identities: treat the root node for this subgraph as the seed
        if (
            "labels" in node
            and this_seed_type in node["labels"]
            and "properties" in node
            and "value" in node["properties"]
            and node["properties"]["value"] == this_seed_value
        ):
            return True
    return False


def write_subgraphs(
    subgraphs: Dict[str, List],
    secrets_file: dict = None,
    seed_identities: Dict[str, List[str]] = None,
) -> None:
    """
    Write all the subgraphs extracted from global GIN to merchant specific GIN.
    Only seed identities have ID values, rest of the nodes are hashed.

    Args:
        subgraphs: List of subgraphs, each containing seed_value, seed_type, nodes, and relationships.
        merchant_neo4j_config: (optional) dict to override config for merchant Neo4j target.
        seed_identities: (optional) Dictionary with keys as id_type and values as seed value lists for recognition.
            If not provided, all unmasked nodes except the root will be masked.
    """

    # Build set of seed values per id_type for fast lookup
    seed_identity_sets = defaultdict(set)
    if seed_identities:
        for typ, values in seed_identities.items():
            seed_identity_sets[typ].update(values)

    # Connect to merchant-specific Neo4j (target write instance)
    merchant_driver = neo4j_connect(secrets_file=secrets_file)

    with merchant_driver.session() as session:
        for subgraph in subgraphs:
            seed_type = subgraph.get("seed_type")
            seed_value = subgraph.get("seed_value")
            nodes = subgraph.get("nodes", [])
            relationships = subgraph.get("relationships", [])

            # Build node id mapping for relationships
            node_id_map = {}
            processed_nodes = []
            for node in nodes:
                # Only unmask if node is a seed node
                if not is_seed_node(node, seed_type, seed_value):
                    masked_node = hash_identity(node)
                else:
                    masked_node = node
                # Neo4j node id is not preserved on insert, so for write purposes use a temp id
                node_key = node["id"]
                node_id_map[node_key] = (
                    masked_node  # This is later used to map relationships
                )

                processed_nodes.append((node_key, masked_node))

            # Write nodes (MERGE for idempotency, using all available label/value)
            for node_key, node in processed_nodes:
                labels = ":".join(node["labels"]) if "labels" in node else ""
                properties = node.get("properties", {})
                # The unique matcher for node: id_type + value if unmasked, otherwise masked value
                # Remove internal Neo4j id if present
                if "id" in properties:
                    del properties["id"]

                # Compose Cypher MERGE based on available information
                props_cypher = ", ".join([f"{k}: ${k}" for k in properties])
                cypher = f"""
                MERGE (n{f":{labels}" if labels else ""} {{ {props_cypher} }})
                SET n += $extra_props
                """
                # To allow for any additional fields that should be set (e.g. relation_count, last_seen)
                extra_props = {}
                for k in node:
                    if k not in {"labels", "properties"}:
                        extra_props[k] = node[k]
                # Merge properties dicts for Cypher binding
                cypher_params = {**properties, "extra_props": extra_props}
                session.run(cypher, **cypher_params)

            # Write relationships
            rel_query_template = """
            MATCH (a { value: $start_value })
            MATCH (b { value: $end_value })
            MERGE (a)-[r:%s]->(b)
            SET r += $props
            """
            for rel in relationships:
                # Get node values for endpoints;
                # Figure out which node corresponds to rel["start"] and rel["end"]
                start_node = node_id_map.get(rel["start"])
                end_node = node_id_map.get(rel["end"])
                if not start_node or not end_node:
                    continue
                start_value = start_node.get("properties", {}).get("value")
                end_value = end_node.get("properties", {}).get("value")
                rel_type = rel["type"]
                rel_props = rel.get("properties", {})
                # Remove internal Neo4j id if present
                if "id" in rel_props:
                    del rel_props["id"]

                cypher = rel_query_template % rel_type
                params = {
                    "start_value": start_value,
                    "end_value": end_value,
                    "props": rel_props,
                }
                session.run(cypher, **params)


def hash_identity(node: Dict[str, str]) -> Dict[str, str]:
    """
    Retain the id type and the relation count as well as the last seen value.
    Mask the value of the ID.
    Returns a new node dict with masked id and selected properties.
    """
    hashed_node = {}

    if "labels" in node:
        hashed_node["labels"] = node["labels"]

    if "properties" in node:
        props = node["properties"].copy()
        if "value" in props:
            value = props["value"]
            masked_value = hashlib.sha256(str(value).encode()).hexdigest()[:10]
            props["value"] = f"masked_{masked_value}"
        hashed_node["properties"] = props

        # Retain relation (edge/degree) count, if present in properties
        if "relation_count" in props:
            hashed_node["relation_count"] = props["relation_count"]

        # Retain last seen value, if present in properties
        if "last_seen" in props:
            hashed_node["last_seen"] = props["last_seen"]

    return hashed_node


def extract_ids_from_logs(logs: pd.DataFrame, event_type: str):
    """
    Extract the IDs from the API logs for a given event type.
    Args:
        logs: pd.DataFrame containing the API logs.
        event_type: str containing the event type.
        processing_function: callable function to process the logs.
    Returns:
        dataframe containing the IDs.
    """
    processing_function = {
        "pan-profile": pan_profile_processing,
    }[event_type]
    return processing_function(logs)


def pan_profile_processing(logs: pd.DataFrame) -> Dict[str, List[str]]:
    """
    Process the PAN profile logs.
    """
    result = {}
    result[IdentityTypes.PAN] = logs.merchantrequestbody.apply(
        lambda x: json.loads(x).get("pan")
    ).to_list()
    result[IdentityTypes.NAME] = logs.response.apply(
        lambda x: json.loads(x).get("name")
    ).to_list()
    return result


def dl_processing(logs: pd.DataFrame) -> Dict[str, List[str]]:
    pass


def phone_processing(logs: pd.DataFrame) -> Dict[str, List[str]]:
    pass


def email_processing(logs: pd.DataFrame) -> Dict[str, List[str]]:
    pass


def rc_processing(logs: pd.DataFrame) -> Dict[str, List[str]]:
    pass
