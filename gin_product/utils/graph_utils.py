from collections import defaultdict
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
from typing import Dict, List, Set, Tuple
import uuid
import warnings

from neo4j import GraphDatabase
import pandas as pd

from utils.logs_processing import IdentityTypes

warnings.filterwarnings("ignore")


class SubgraphExtractor:
    def __init__(
        self,
        secrets: dict,
        logger: logging.Logger,
        batch_size: int = 100,
        max_depth: int = 2,
    ):
        """Initialize connection to Neo4j database"""
        self.driver = GraphDatabase.driver(**secrets["read_neo4j"])
        self.logger: logging.Logger = logger
        self.batch_size = batch_size
        self.max_depth = max_depth

    def close(self):
        """Close the database connection"""
        self.driver.close()

    def extract_subgraph_with_terminators(
        self,
        seeds: Dict[str, pd.DataFrame],
        max_depth: int = None,
    ) -> Dict[str, List[Dict]]:

        if max_depth is None:
            max_depth = self.max_depth

        all_nodes = {}  # Deduplicate by Neo4j ID
        all_relationships = {}

        for event_type, event_data in seeds.items():
            node_types = event_data.drop(columns=["requestTimestamp"]).columns.tolist()
            for node_type in node_types:
                # Batch node_values by batch_size for processing
                node_values = event_data[node_type].values
                total = len(node_values)
                for i in range(0, total, self.batch_size):
                    node_values_batch = node_values[i : i + self.batch_size]
                    batch_nodes, batch_rels = self._process_batch(
                        node_type, node_values_batch, max_depth
                    )
                    # Add request timestamp to the nodes
                    for node in batch_nodes.values():
                        # Find matching timestamp from the original batch DataFrame
                        request_timestamp = event_data[
                            (event_data[node_type] == node["properties"]["value"])
                        ]["requestTimestamp"]
                        if not request_timestamp.empty:
                            node["properties"]["requestTimestamp"] = (
                                request_timestamp.values[0]
                            )

                    all_nodes.update(batch_nodes)
                    all_relationships.update(batch_rels)

        return {
            "nodes": list(all_nodes.values()),
            "relationships": list(all_relationships.values()),
        }

    def _process_batch(
        self,
        node_type: str,
        node_values: List[str],
        max_depth: int = None,
    ) -> Tuple[Dict, Dict]:

        self.logger.info(
            "Processing batch of %d nodes for node type `%s` with max depth %d",
            len(node_values),
            node_type,
            max_depth,
        )

        if max_depth is None:
            max_depth = self.max_depth

        cquery = f"""
            UNWIND $seed_values AS node_value
            MATCH (start:{node_type} {{value: node_value}})

            CALL apoc.path.subgraphAll(start, {{
                labelFilter: "-publicIP|-fullName|-completeAddress",
                maxLevel: {max_depth}
            }}) YIELD nodes, relationships

            UNWIND nodes AS n
            OPTIONAL MATCH (n)-[r]-(terminator)
            WHERE terminator:fullName OR terminator:publicIP OR terminator:completeAddress

            WITH 
                collect(DISTINCT n) AS coreNodes,
                collect(DISTINCT terminator) AS termNodes,
                collect(DISTINCT r) AS termRels,
                relationships AS coreRels

            RETURN 
                [x IN (coreNodes + termNodes) WHERE x IS NOT NULL] AS nodes,
                [x IN (coreRels + termRels) WHERE x IS NOT NULL] AS relationships
        """

        batch_nodes = {}
        batch_relationships = {}

        with self.driver.session() as session:
            result = session.run(
                cquery,
                seed_values=node_values,
                max_depth=max_depth,
            )
            record = result.single()
            if not record:
                return batch_nodes, batch_relationships

            for node in record["nodes"]:
                batch_nodes[node.element_id] = {
                    "id": node.element_id,
                    "label": sorted(node.labels)[0],
                    "properties": {
                        "value": node["value"],
                        "firstSeen": node["firstSeen"],
                        "lastSeen": node["lastSeen"],
                        "fraudCount": node.get("fraudCount", 0),
                    },
                }
            for rel in record["relationships"]:
                batch_relationships[rel.element_id] = {
                    "id": rel.element_id,
                    "type": rel.type,
                    "start_node_id": rel.start_node.element_id,
                    "end_node_id": rel.end_node.element_id,
                    "properties": {
                        "count": rel["relCount"],
                        "lastSeen": rel["lastSeen"],
                        "firstSeen": rel["firstSeen"],
                    },
                }

        return batch_nodes, batch_relationships

    def export_to_cypher(
        self,
        nodes: List[Dict],
        relationships: List[Dict],
        output_file: str = "exported_subgraph.cypher",
    ):
        """Export nodes and relationships to Cypher CREATE statements"""

        with open(output_file, "w", encoding="utf-8") as f:
            # Write header
            f.write("// Neo4j Subgraph Export\n")
            f.write(f"// Total nodes: {len(nodes)}\n")
            f.write(f"// Total relationships: {len(relationships)}\n\n")

            # Create nodes
            f.write("// Create nodes\n")
            for node in nodes:
                labels_str = ":".join(
                    [""] + node["labels"]
                )  # Start with : for first label
                props_str = self._format_properties(node["properties"])

                f.write(f"CREATE (n{node['id']}{labels_str}")
                if props_str:
                    f.write(f" {props_str}")
                f.write(");\n")

            f.write("\n// Create relationships\n")
            # Create relationships
            for rel in relationships:
                props_str = self._format_properties(rel["properties"])

                f.write(
                    f"MATCH (a), (b) WHERE id(a) = {rel['start_node_id']} AND id(b) = {rel['end_node_id']} "
                )
                f.write(f"CREATE (a)-[:{rel['type']}")
                if props_str:
                    f.write(f" {props_str}")
                f.write("]->(b);\n")

        print(
            f"Exported {len(nodes)} nodes and {len(relationships)} relationships to {output_file}"
        )

    def export_to_json(
        self,
        nodes: List[Dict],
        relationships: List[Dict],
        output_file: str = "exported_subgraph.json",
    ):
        """Export to JSON format"""
        export_data = {
            "nodes": nodes,
            "relationships": relationships,
            "metadata": {
                "node_count": len(nodes),
                "relationship_count": len(relationships),
            },
        }

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(export_data, f, indent=2, default=str)

        print(f"Exported to JSON: {output_file}")

    def _format_properties(self, properties: Dict) -> str:
        """Format properties dictionary as Cypher map syntax"""
        if not properties:
            return ""

        formatted_props = {}
        for key, value in properties.items():
            if isinstance(value, str):
                escaped_value = value.replace("'", "\\'").replace('"', '\\"')
                formatted_props[key] = f"'{escaped_value}'"
            elif isinstance(value, (int, float, bool)):
                formatted_props[key] = (
                    str(value).lower() if isinstance(value, bool) else str(value)
                )
            elif isinstance(value, list):
                formatted_list = []
                for item in value:
                    if isinstance(item, str):
                        escaped_item = item.replace("'", "\\'").replace('"', '\\"')
                        formatted_list.append(f"'{escaped_item}'")
                    else:
                        formatted_list.append(str(item))
                formatted_props[key] = f"[{', '.join(formatted_list)}]"
            else:
                formatted_props[key] = f"'{str(value)}'"

        props_str = ", ".join(
            [f"{key}: {value}" for key, value in formatted_props.items()]
        )
        return f"{{{props_str}}}"


class SubgraphWriter:
    def __init__(self, secrets: dict, logger: logging.Logger, batch_size: int = 10000):
        """Initialize connection to Neo4j database"""
        self.logger: logging.Logger = logger
        self.batch_size = batch_size
        try:
            self.driver = GraphDatabase.driver(**secrets["write_neo4j"])
        except Exception as e:
            self.logger.error(f"Error initializing Neo4j connection: {e}")
            print(f"Will not be able to write to Neo4j, will export as json instead")
            self.export_as_json = True
        else:
            self.export_as_json = False

    def close(self):
        """Close the database connection"""
        self.driver.close()

    @staticmethod
    def hash_identity(node: dict, is_seed_node: bool) -> dict:
        """
        Given a node dictionary, mask its 'value' field in properties, preserving labels and id.
        """
        node_copy = dict(node)
        node_copy = {
            k: (v.copy() if isinstance(v, dict) else v) for k, v in node_copy.items()
        }

        # Mask value in properties if present
        if "properties" in node_copy and "value" in node_copy["properties"]:
            original_val = node_copy["properties"]["value"]
            # Use SHA256 for hashing
            hashed_val = hashlib.sha256(str(original_val).encode("utf-8")).hexdigest()
            node_copy["properties"]["hashed_value"] = hashed_val

        if is_seed_node or node_copy["label"] == IdentityTypes.NAME.value:
            return node_copy
        node_copy["properties"].pop("value", None)
        return node_copy

    @staticmethod
    def get_seeds(seed_identities: Dict[str, pd.DataFrame]) -> Set[str]:
        """
        Get the seeds for a given event type.
        """
        seeds = set()
        for event_type, seed_df in seed_identities.items():
            identity_types = seed_df.drop(columns=["requestTimestamp"]).columns.tolist()
            for identity_type in identity_types:
                seeds.update(set(seed_df[identity_type].values.tolist()))
        return seeds

    @staticmethod
    def _connected_component_ids(
        nodes: List[dict], relationships: List[dict]
    ) -> Dict[str, str]:
        """
        Compute a component_id (ring id) for each node so that all nodes in the same
        connected component get the same id. Uses Union-Find on the in-memory graph.

        Returns:
            Map from node id (element_id) to component_id (UUID string).
        """
        parent: Dict[str, str] = {}

        def find(x: str) -> str:
            if x not in parent:
                parent[x] = x
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]

        def union(x: str, y: str) -> None:
            px, py = find(x), find(y)
            if px != py:
                parent[px] = py

        for node in nodes:
            find(node["id"])
        for rel in relationships:
            start_id = rel.get("start_node_id")
            end_id = rel.get("end_node_id")
            if start_id and end_id:
                union(start_id, end_id)

        roots = {find(n["id"]) for n in nodes}
        root_to_uuid: Dict[str, str] = {r: str(uuid.uuid4()) for r in roots}
        return {n["id"]: root_to_uuid[find(n["id"])] for n in nodes}

    def write_subgraphs(
        self, subgraph: Dict[str, List], seed_identities: Dict[str, pd.DataFrame]
    ) -> None:
        """
        Write subgraph nodes and relationships to Neo4j. Each node gets a component_id
        (ring id) so that all nodes in the same connected component share the same id.
        New components get a new id; components that merge with existing nodes are
        unified in a post-write step.
        """
        seeds = self.get_seeds(seed_identities)

        node_id_map = {}
        node_rows: List[Tuple[str, str, dict]] = []  # (node_key, label, props)

        for node in subgraph["nodes"]:
            is_seed_node = node["properties"]["value"] in seeds
            masked_node = self.hash_identity(node, is_seed_node)

            props = masked_node.get("properties", {}).copy()
            props.pop("id", None)

            node_key = masked_node["id"]
            node_id_map[node_key] = {
                "label": masked_node["label"],
                "hashed_value": props.get("hashed_value"),
            }
            node_rows.append((node_key, masked_node["label"], props))

        # Assign component_id so all nodes in the same in-memory component share one id
        component_ids = self._connected_component_ids(
            subgraph["nodes"], subgraph["relationships"]
        )
        nodes_by_label: Dict[str, List[dict]] = defaultdict(list)
        for node_key, label_key, props in node_rows:
            props["component_id"] = component_ids[node_key]
            nodes_by_label[label_key].append(props)

        rels_by_type = defaultdict(list)
        for rel in subgraph["relationships"]:
            start = node_id_map.get(rel["start_node_id"])
            end = node_id_map.get(rel["end_node_id"])

            if start and end:
                rel_props = rel.get("properties", {}).copy()
                rel_props.pop("id", None)

                rels_by_type[rel["type"]].append(
                    {
                        "start_label": start["label"],
                        "start_value": start["hashed_value"],
                        "end_label": end["label"],
                        "end_value": end["hashed_value"],
                        "props": rel_props,
                    }
                )

        with self.driver.session() as session:
            for labels, nodes in nodes_by_label.items():
                for i in range(0, len(nodes), self.batch_size):
                    batch = nodes[i : i + self.batch_size]
                    self.logger.info(
                        f"Writing nodes with labels {labels} and batch size {len(batch)}"
                    )
                    # On CREATE set full row; ON MATCH keep existing component_id
                    node_cypher = f"""
                    UNWIND $batch AS row
                    MERGE (n:{labels}:Identity {{ hashed_value: row.hashed_value}})
                    ON CREATE SET n += row
                    ON MATCH SET n += apoc.map.removeKey(row, 'component_id'),
                                   n.component_id = coalesce(n.component_id, row.component_id)
                    """
                    session.run(node_cypher, batch=batch)

            for rel_type, relationships in rels_by_type.items():
                for i in range(0, len(relationships), self.batch_size):
                    batch = relationships[i : i + self.batch_size]
                    self.logger.info(
                        f"Writing relationships with type {rel_type} and batch size {len(batch)}"
                    )
                    rel_cypher = f"""
                        UNWIND $batch AS row
                        CALL apoc.cypher.run(
                            "MATCH (n:" + row.start_label + " {{hashed_value: $val}}) RETURN n", 
                            {{val: row.start_value}}
                        ) YIELD value AS resA
                        CALL apoc.cypher.run(
                            "MATCH (m:" + row.end_label + " {{hashed_value: $val}}) RETURN m", 
                            {{val: row.end_value}}
                        ) YIELD value AS resB
                        WITH resA.n AS a, resB.m AS b, row
                        MERGE (a)-[r:{rel_type}]->(b)
                        SET r += row.props
                    """
                    session.run(rel_cypher, batch=batch)

            # Unify component_id so components that merged with existing nodes get one id.
            # Iterate: set each node's component_id to min of neighbors' until fixpoint.
            self._merge_component_ids(session)

    def _merge_component_ids(self, session) -> None:
        """
        Unify component_id across connected nodes. When new nodes were MERGED with
        existing nodes, the component may have mixed ids. Repeatedly set each node's
        component_id to the minimum of its neighbors' until no changes.
        """
        merge_cypher = """
            MATCH (n)-[]-(m)
            WHERE n.component_id IS NOT NULL AND m.component_id IS NOT NULL
              AND n.component_id > m.component_id
            WITH n, min(m.component_id) AS newId
            SET n.component_id = newId
            RETURN count(n) AS updated
        """
        updated = 1  # Ensure the loop runs at least once
        while updated > 0:
            result = session.run(merge_cypher)
            record = result.single()
            updated = record["updated"] if record else 0
            if updated > 0:
                self.logger.debug("Unified component_id for %d nodes", updated)
            else:
                break


class SubgraphCleaner:
    """
    This class is used to clean the subgraph by removing node values
    for nodes which have request timestamp older than `merchant_data_retention_days` days.
    This class will be instantiated as part of a automated cron job to clean the subgraphs.
    """

    def __init__(self, secrets: dict, merchant_conf: dict):
        """Initialize connection to Neo4j and retention policy."""
        self.merchant_data_retention_days = merchant_conf[
            "merchant_data_retention_days"
        ]
        self.driver = GraphDatabase.driver(**secrets["write_neo4j"])

    def close(self):
        """Close the database connection."""
        self.driver.close()

    def clean(self) -> int:
        """
        Remove the `value` property from nodes whose requestTimestamp is older
        than merchant_data_retention_days. Nodes and relationships are left intact.
        Returns the number of nodes updated.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(
            days=self.merchant_data_retention_days
        )
        query = """
            MATCH (n)
            WHERE n.requestTimestamp IS NOT NULL
              AND n.requestTimestamp < $cutoff
              AND n.value IS NOT NULL
            WITH n
            REMOVE n.value
            REMOVE n.requestTimestamp
            RETURN count(n) AS updated
        """
        with self.driver.session() as session:
            result = session.run(query, cutoff=cutoff)
            record = result.single()
            return record["updated"] if record else 0


class GraphInitializer:
    """
    This class will be used as a one time setup script to create the graph schema and indexes.
    """

    def __init__(
        self,
        secrets: dict,
        logger: logging.Logger,
        do_clear_graph: bool = False,
        batch_size: int = 10000,
    ):
        """Initialize connection to Neo4j database"""
        self.driver = GraphDatabase.driver(**secrets["write_neo4j"])
        self.logger: logging.Logger = logger
        self.do_clear_graph = do_clear_graph
        self.batch_size = batch_size

    def close(self):
        """Close the database connection"""
        self.driver.close()

    def setup(self) -> None:
        """Create the graph schema and indexes"""
        if self.do_clear_graph:
            self.logger.info(f"Do you want to clear the graph? (y/n)")
            if input() == "y":
                self.clear_graph()
            else:
                self.logger.info("Aborting graph setup")
                return

        with self.driver.session() as session:
            for identity_type in IdentityTypes:
                session.run(
                    f"CREATE INDEX {identity_type.value}_hashed_index IF NOT EXISTS FOR (n:{identity_type.value}) ON (n.hashed_value)"
                )
            session.run(
                "CREATE INDEX IF NOT EXISTS FOR (n:Identity) ON (n.component_id)"
            )

    def clear_graph(self) -> None:
        """
        Clear the graph in batches to avoid overwhelming the database.
        """
        query = f"""
            MATCH (n)
            WITH n LIMIT $batch_size
            DETACH DELETE n
            RETURN count(n) as deleted_count
        """
        drop_indexes_query = """
        call apoc.schema.assert({},{})
        """
        with self.driver.session() as session:
            total_deleted = 0
            while True:
                result = session.run(query, batch_size=self.batch_size)
                record = result.single()
                deleted_count = record["deleted_count"] if record else 0
                total_deleted += deleted_count
                if deleted_count == 0:
                    break
            self.logger.info(f"Cleared {total_deleted} nodes")
            session.run(drop_indexes_query)
