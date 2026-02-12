"""
This module contains all functions to extract features from the merchant neo4j graph instance.
Functions include:
- node_centrality
- connected_component_count (by node type)
- Additional network based features
Scope:
- nodes ingested in the last 24 hours

Designed for daily cron execution; features are written to CSV.
"""

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
from neo4j import GraphDatabase


def _union_find_components(edges: List[Tuple[int, int]]) -> Dict[int, int]:
    """Compute connected components via Union-Find. Returns node_id -> component_id."""
    parent: Dict[int, int] = {}

    def find(x: int) -> int:
        if x not in parent:
            parent[x] = x
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]

    def union(x: int, y: int) -> None:
        px, py = find(x), find(y)
        if px != py:
            parent[px] = py

    for a, b in edges:
        union(a, b)

    # Normalize so each component has a canonical id (min node id in component)
    comp_id = {}
    for node in parent:
        comp_id[node] = find(node)
    return comp_id


class GraphFeatures:
    def __init__(self, secrets: dict, hours_window: int = 24):
        self.driver = GraphDatabase.driver(**secrets["write_neo4j"])
        self.hours_window = hours_window

    def close(self) -> None:
        self.driver.close()

    def _cutoff(self) -> datetime:
        return datetime.now(timezone.utc) - timedelta(hours=self.hours_window)

    def _get_nodes_with_degree(self) -> pd.DataFrame:
        """Return nodes ingested in the last hours_window with degree centrality."""
        cutoff = self._cutoff()
        query = """
            MATCH (n)
            WHERE n.requestTimestamp IS NOT NULL
              AND n.requestTimestamp >= $cutoff
            OPTIONAL MATCH (n)-[r]-()
            WITH n, count(r) AS degree
            RETURN id(n) AS node_id,
                   labels(n) AS labels,
                   n.hashed_value AS hashed_value,
                   n.requestTimestamp AS request_timestamp,
                   degree
        """
        with self.driver.session() as session:
            result = session.run(query, cutoff=cutoff)
            rows = [dict(record) for record in result]
        if not rows:
            return pd.DataFrame(
                columns=[
                    "node_id",
                    "labels",
                    "hashed_value",
                    "request_timestamp",
                    "degree",
                ]
            )
        return pd.DataFrame(rows)

    def _get_edges_for_recent_nodes(self) -> List[Tuple[int, int]]:
        """Return edges where at least one endpoint was ingested in the window (for WCC)."""
        cutoff = self._cutoff()
        query = """
            MATCH (a)-[r]-(b)
            WHERE (a.requestTimestamp IS NOT NULL AND a.requestTimestamp >= $cutoff)
               OR (b.requestTimestamp IS NOT NULL AND b.requestTimestamp >= $cutoff)
            RETURN id(a) AS a_id, id(b) AS b_id
        """
        with self.driver.session() as session:
            result = session.run(query, cutoff=cutoff)
            return [(record["a_id"], record["b_id"]) for record in result]

    def _connected_component_counts_by_type(
        self, node_df: pd.DataFrame, edges: List[Tuple[int, int]]
    ) -> Dict[str, int]:
        """
        For each node type (label), count how many connected components contain
        at least one node of that type (among nodes in the time window).
        """
        if node_df.empty:
            return {}
        node_ids = set(node_df["node_id"].astype(int))
        comp = _union_find_components(edges)
        # component_id -> set of labels present in that component (from window nodes only)
        comp_labels: Dict[int, Set[str]] = defaultdict(set)
        for _, row in node_df.iterrows():
            nid = int(row["node_id"])
            if nid not in comp:
                comp[nid] = nid
            cid = comp[nid]
            for label in row["labels"] or []:
                comp_labels[cid].add(label)
        # For each label, count components that contain at least one node with that label
        label_component_count: Dict[str, int] = defaultdict(int)
        for labels in comp_labels.values():
            for label in labels:
                label_component_count[label] += 1
        return dict(label_component_count)

    def extract_features(
        self,
    ) -> Tuple[pd.DataFrame, List[Tuple[int, int]]]:
        """
        Extract node-level features for nodes ingested in the last 24 hours:
        node_id, labels, hashed_value, request_timestamp, degree, component_id.
        Returns (node_df, edges) so callers can reuse edges for aggregation.
        """
        node_df = self._get_nodes_with_degree()
        edges = self._get_edges_for_recent_nodes() if not node_df.empty else []
        if node_df.empty:
            node_df["component_id"] = pd.Series(dtype="Int64")
            return node_df, edges

        comp = _union_find_components(edges)
        node_df["component_id"] = node_df["node_id"].map(
            lambda nid: comp.get(int(nid), int(nid))
        )
        return node_df, edges

    def compute_merchant_aggregated_features(
        self, df: pd.DataFrame, edges: Optional[List[Tuple[int, int]]] = None
    ) -> pd.DataFrame:
        """
        Aggregate node-level features into one row per run (merchant-level summary).
        Includes degree stats, component counts by node type, and network stats.

        Args:
            df: Node-level features from extract_features().
            edges: Optional pre-fetched edge list; if None, will be queried.
        """
        if df.empty:
            return self._empty_aggregated_row()

        if edges is None:
            edges = self._get_edges_for_recent_nodes()
        component_counts_by_type = self._connected_component_counts_by_type(df, edges)

        # Degree stats
        degree = df["degree"]
        agg = {
            "feature_date": datetime.now(timezone.utc).date().isoformat(),
            "feature_run_utc": datetime.now(timezone.utc).isoformat(),
            "nodes_in_window": len(df),
            "edges_in_window": len(edges),
            "degree_mean": float(degree.mean()),
            "degree_std": float(degree.std()) if len(degree) > 1 else 0.0,
            "degree_min": int(degree.min()),
            "degree_max": int(degree.max()),
            "degree_median": float(degree.median()),
            "unique_components": (
                int(df["component_id"].nunique()) if "component_id" in df.columns else 0
            ),
        }
        for label, count in component_counts_by_type.items():
            agg[f"components_with_{label}"] = count

        return pd.DataFrame([agg])

    def _empty_aggregated_row(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "feature_date": datetime.now(timezone.utc).date().isoformat(),
                    "feature_run_utc": datetime.now(timezone.utc).isoformat(),
                    "nodes_in_window": 0,
                    "edges_in_window": 0,
                    "degree_mean": 0.0,
                    "degree_std": 0.0,
                    "degree_min": 0,
                    "degree_max": 0,
                    "degree_median": 0.0,
                    "unique_components": 0,
                }
            ]
        )

    def run_and_write_csv(
        self,
        output_path: str,
        include_node_level: bool = False,
        node_level_path: Optional[str] = None,
    ) -> None:
        """
        Extract features, compute merchant-level aggregates, and write to CSV.
        Suitable for daily cron: append one row to the aggregated CSV per run.

        Args:
            output_path: Path for aggregated (merchant-level) features CSV.
            include_node_level: If True, also write node-level features to CSV.
            node_level_path: Path for node-level CSV. Used only if include_node_level is True.
        """
        node_df, edges = self.extract_features()
        agg_df = self.compute_merchant_aggregated_features(node_df, edges=edges)

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        # Append aggregated row; write header only when creating a new file
        file_exists = (
            Path(output_path).exists() and Path(output_path).stat().st_size > 0
        )
        agg_df.to_csv(
            output_path,
            mode="a",
            header=not file_exists,
            index=False,
        )

        if include_node_level and node_level_path:
            Path(node_level_path).parent.mkdir(parents=True, exist_ok=True)
            node_df.to_csv(node_level_path, index=False)
