import hashlib
import os
from typing import Dict, List

import yaml

from neo4j import GraphDatabase
from pyathena import connect


def load_secrets(secrets_file: str = "secrets.yml") -> dict:
    """Load secrets from a YAML file."""
    if not os.path.exists(secrets_file):
        raise FileNotFoundError(f"Secrets file not found: {secrets_file}")
    with open(secrets_file, "r") as f:
        secrets = yaml.safe_load(f)
    return secrets or {}


def neo4j_connect(config=None, secrets_file: str = "secrets.yml"):
    """
    Returns a valid neo4j connection object using credentials from secrets file.

    If config is provided, it overrides values in secrets file.
    """

    secrets = load_secrets(secrets_file)
    cfg = {**secrets.get("neo4j", {}), **(config or {})}

    uri = cfg.get("uri") or cfg.get("bolt_url") or cfg.get("host")
    user = cfg.get("user") or cfg.get("username")
    password = cfg.get("password")
    encrypted = cfg.get("encrypted", False)

    # Fallback for host/port if uri is not provided
    if not uri and "host" in cfg and "port" in cfg:
        uri = f"bolt://{cfg['host']}:{cfg['port']}"

    driver = GraphDatabase.driver(
        uri,
        auth=(user, password),
        encrypted=encrypted,
    )
    return driver


def athena_connect(config=None, secrets_file: str = "secrets.yml"):
    """
    Returns a valid AWS Athena connection object using credentials from secrets file.

    Config should include override keys: aws_access_key_id, aws_secret_access_key, region_name, s3_staging_dir, database.
    """

    secrets = load_secrets(secrets_file)
    cfg = {**secrets.get("athena", {}), **(config or {})}

    aws_access_key_id = cfg.get("aws_access_key_id")
    aws_secret_access_key = cfg.get("aws_secret_access_key")
    region_name = cfg.get("region_name", "us-east-1")
    s3_staging_dir = cfg.get("s3_staging_dir")
    database = cfg.get("database")

    conn = connect(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
        s3_staging_dir=s3_staging_dir,
        schema_name=database,
    )
    return conn


def retrive_identities(
    merchant_id: str,
    start_date: str = None,
    end_date: str = None,
    data_retention_days: int = None,
):
    """
    Query all identities for a merchant_id for a given time frame from the API call logs.
    """
    pass


def subgraph_query(id_type: str, ids: list, max_depth=4, batch_size=1000) -> list:
    """
    Query subgraphs for each seed identity up to a specified maximum depth, from global GIN.

    Processes ids in batches, improving query efficiency for large lists.

    Args:
        id_type (str): The type of identity node (e.g., "email", "phone").
        ids (List[str]): List of seed identity values.
        max_depth (int): Maximum BFS traversal depth.
        batch_size (int): Number of seeds per Cypher batch.

    Returns:
        List[Dict]: List of subgraphs (each subgraph as list of nodes/edges for each id).
    """

    driver = neo4j_connect()
    results = []

    cypher_template = f"""
    UNWIND $seed_values AS seed_value
    MATCH (seed:{id_type} {{value: seed_value}})
    CALL apoc.path.subgraphAll(seed, {{
        maxLevel: $max_depth
    }}) YIELD nodes, relationships
    RETURN
        seed.value AS seed_value,
        [n IN nodes | {{
            id: id(n),
            labels: labels(n),
            properties: properties(n)
        }}] AS nodes,
        [r IN relationships | {{
            id: id(r),
            type: type(r),
            start: startNode(r).id,
            end: endNode(r).id,
            properties: properties(r)
        }}] AS relationships
    """

    with driver.session() as session:
        for i in range(0, len(ids), batch_size):
            batch = ids[i : i + batch_size]
            records = session.run(
                cypher_template, seed_values=batch, max_depth=max_depth
            )
            for record in records:
                results.append(
                    {
                        "seed_type": id_type,
                        "seed_value": record["seed_value"],
                        "nodes": record["nodes"],
                        "relationships": record["relationships"],
                    }
                )

    return results


def retrive_subgraphs(
    seed_identities: Dict[str, List[str]], max_depth=4, batch_size=1000
) -> List[Dict[str, str]]:
    """
    Fetch all subgraphs for each identity type present in the input seed_identities.

    Args:
        seed_identities: Dictionary with keys as id_type and values as list of seed ids, e.g. {'email': [...], 'phone': [...]}
        max_depth: Maximum BFS traversal depth for each seed

    Returns:
        List of subgraph dicts (one per seed id)
    """
    all_subgraphs = []
    for id_type, ids in seed_identities.items():
        subgraphs = subgraph_query(id_type, ids, max_depth=max_depth)
        all_subgraphs.extend(subgraphs)
    return all_subgraphs


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
    subgraphs: list,
    merchant_neo4j_config: dict = None,
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
    from collections import defaultdict

    # Build set of seed values per id_type for fast lookup
    seed_identity_sets = defaultdict(set)
    if seed_identities:
        for typ, values in seed_identities.items():
            seed_identity_sets[typ].update(values)

    # Connect to merchant-specific Neo4j (target write instance)
    merchant_driver = neo4j_connect(
        config=merchant_neo4j_config, secrets_file="secrets.yml"
    )

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
