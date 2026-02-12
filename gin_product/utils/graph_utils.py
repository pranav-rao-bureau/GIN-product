from collections import defaultdict
from datetime import datetime, timedelta, timezone
import hashlib
import json
from typing import Dict, List, Set, Tuple

from neo4j import GraphDatabase
import pandas as pd

from gin_product.utils.logs_processing import IdentityTypes


class SubgraphExtractor:
    def __init__(self, secrets: dict, batch_size: int = 100, max_depth: int = 3):
        """Initialize connection to Neo4j database"""
        self.driver = GraphDatabase.driver(**secrets["read_neo4j"])
        self.batch_size = batch_size
        self.max_depth = max_depth

    def close(self):
        """Close the database connection"""
        self.driver.close()

    def extract_subgraph_with_terminators(
        self,
        seeds: Dict[str, pd.DataFrame],
        terminator_labels: List[str] = None,
        max_depth: int = None,
    ) -> Dict[str, List[Dict]]:
        if terminator_labels is None:
            terminator_labels = ["fullName", "publicIP", "completeAddress"]

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
                        node_type, node_values_batch, terminator_labels, max_depth
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
        terminator_labels: List[str],
        max_depth: int = None,
    ) -> Tuple[Dict, Dict]:
        label_filter = "|".join([f"-{label}" for label in terminator_labels])

        if max_depth is None:
            max_depth = self.max_depth

        cquery = (
            f"""
            UNWIND $seed_values AS node_value
            MATCH (start:`{node_type}` {{value: node_value}})
            CALL apoc.path.subgraphAll(start, {{
                labelFilter: $labelFilter,
                maxLevel: $maxDepth
            }}) YIELD nodes as traversableNodes, relationships as traversableRels
            WITH collect(traversableNodes) as allNodeCollections,
                 collect(traversableRels) as allRelCollections

            WITH apoc.coll.toSet(apoc.coll.flatten(allNodeCollections)) as uniqueNodes,
                 apoc.coll.toSet(apoc.coll.flatten(allRelCollections)) as uniqueRels

            UNWIND uniqueNodes as n
            OPTIONAL MATCH (n)-[r]-(terminator)
            WHERE """
            + " OR ".join([f"terminator:{label}" for label in terminator_labels])
            + """

            WITH uniqueNodes, uniqueRels,
                 apoc.coll.toSet(collect(terminator)) as terminatorNodes,
                 apoc.coll.toSet(collect(r)) as terminatorRels

            WITH uniqueNodes + [x IN terminatorNodes WHERE x IS NOT NULL] as finalNodes,
                 uniqueRels + [x IN terminatorRels WHERE x IS NOT NULL] as finalRels

            RETURN finalNodes as nodes, finalRels as relationships
            """
        )

        batch_nodes = {}
        batch_relationships = {}

        with self.driver.session() as session:
            result = session.run(
                cquery,
                seed_values=node_values,
                labelFilter=label_filter,
                maxDepth=max_depth,
            )
            record = result.single()
            if not record:
                return batch_nodes, batch_relationships

            for node in record["nodes"]:
                batch_nodes[node.id] = {
                    "id": node.id,
                    "labels": list(node.labels),
                    "properties": dict(node),
                }
            for rel in record["relationships"]:
                batch_relationships[rel.id] = {
                    "id": rel.id,
                    "type": rel.type,
                    "start_node_id": rel.start_node.id,
                    "end_node_id": rel.end_node.id,
                    "properties": dict(rel),
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
    def __init__(self, secrets: dict):
        """Initialize connection to Neo4j database"""
        try:
            self.driver = GraphDatabase.driver(**secrets["write_neo4j"])
        except Exception as e:
            print(f"Error initializing Neo4j connection: {e}")
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

        # if not is_seed_node:
        #     del node_copy["properties"]["value"]

        return node_copy

    @staticmethod
    def is_seed_node(node: dict, seed_identities: Dict[str, Set[str]]) -> bool:
        """
        Determine if a given node is a seed node, based on type/value, and optionally, a set of allowed seed values for each id_type.
        """
        for typ, seedvals in seed_identities.items():
            if (
                "labels" in node
                and typ in node["labels"]
                and "properties" in node
                and "value" in node["properties"]
            ):
                return node["properties"]["value"] in seedvals
        return False

    def write_subgraphs(
        self,
        subgraph: Dict[str, List],
        seed_identities: Dict[str, pd.DataFrame],
    ) -> None:
        """
        Write all the subgraphs extracted from global GIN to merchant specific GIN.
        Only seed identities have ID values, rest of the nodes are hashed.

        Args:
            subgraphs: List of subgraphs, each containing seed_value, seed_type, nodes, and relationships.
            seed_identities: (optional) Dictionary with keys as id_type and values as seed value lists for recognition.
                If not provided, all unmasked nodes except the root will be masked.
        """

        # Build set of seed values per id_type for fast lookup
        seed_identity_sets = defaultdict(set)
        if seed_identities:
            for typ, values in seed_identities.items():
                seed_identity_sets[typ].update(values)

        with self.driver.session() as session:
            nodes = subgraph["nodes"]
            relationships = subgraph["relationships"]

            node_id_map = {}
            processed_nodes = []
            for node in nodes:
                is_seed_node = self.is_seed_node(node, seed_identity_sets)
                masked_node = self.hash_identity(node, is_seed_node)
                # Neo4j node id is not preserved on insert, so for write purposes use a temp id
                node_key = masked_node["id"]
                node_id_map[node_key] = masked_node
                processed_nodes.append((node_key, masked_node))

            # Write nodes (MERGE for idempotency, using all available label/value)
            for node_key, node in processed_nodes:
                labels = ":".join(node["labels"]) if "labels" in node else ""
                properties = node.get("properties", {})
                # The unique matcher for node: id_type + value if unmasked, otherwise masked value
                # Remove internal Neo4j id if present
                properties = properties.copy()
                if "id" in properties:
                    del properties["id"]

                # Compose Cypher MERGE based on available information
                props_cypher = ", ".join([f"{k}: ${k}" for k in properties])
                cypher = f"""
                MERGE (n{f":{labels}" if labels else ""} {{ {props_cypher} }})
                SET n += $extra_props
                """
                session.run(cypher, **properties, extra_props=properties)

            # Write relationships
            for rel in relationships:
                start_node = node_id_map.get(rel["start_node_id"])
                end_node = node_id_map.get(rel["end_node_id"])
                if not start_node or not end_node:
                    continue

                rel_type = rel["type"]
                rel_properties = rel.get("properties", {}).copy()
                if "id" in rel_properties:
                    del rel_properties["id"]
                cypher = (
                    f"""
                MATCH (a), (b)
                WHERE """
                    + " AND ".join(
                        [
                            f"all(label IN $start_labels WHERE label IN labels(a))",
                            f"all(label IN $end_labels WHERE label IN labels(b))",
                            "a.hashed_value = $start_value",
                            "b.hashed_value = $end_value",
                        ]
                    )
                    + f"""
                MERGE (a)-[r:{rel_type}]->(b)
                SET r += $rel_properties
                """
                )
                session.run(
                    cypher,
                    start_labels=start_node["labels"],
                    end_labels=end_node["labels"],
                    start_value=start_node["properties"]["hashed_value"],
                    end_value=end_node["properties"]["hashed_value"],
                    rel_properties=rel_properties,
                )


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
        self, secrets: dict, do_clear_graph: bool = False, batch_size: int = 10000
    ):
        """Initialize connection to Neo4j database"""
        self.driver = GraphDatabase.driver(**secrets["write_neo4j"])
        self.do_clear_graph = do_clear_graph
        self.batch_size = batch_size

    def close(self):
        """Close the database connection"""
        self.driver.close()

    def setup(self) -> None:
        """Create the graph schema and indexes"""
        identity_types = [identity_type.value for identity_type in IdentityTypes]
        if self.do_clear_graph:
            print(f"Do you want to clear the graph? (y/n)")
            if input() == "y":
                self.clear_graph()
            else:
                print("Aborting graph setup")
                return

        with self.driver.session() as session:
            for identity_type in identity_types:
                session.run(
                    "CREATE INDEX IF NOT EXISTS FOR (n:{0}) ON (n.hashed_value)".format(
                        identity_type
                    )
                )
                session.run(
                    "CREATE INDEX IF NOT EXISTS FOR (n:{0}) ON (n.requestTimestamp)".format(
                        identity_type
                    )
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
        with self.driver.session() as session:
            total_deleted = 0
            while True:
                result = session.run(query, batch_size=self.batch_size)
                record = result.single()
                deleted_count = record["deleted_count"] if record else 0
                total_deleted += deleted_count
                if deleted_count == 0:
                    break
            print(f"Cleared {total_deleted} nodes")
