"""
This module contains all functions to extract features from the merchant neo4j graph instance.
Functions include:
- node_centrality
- connected_component_count (by node type)
- Additional network based features
Scope:
- nodes ingested in the last 24 hours

Designed for daily cron execution; features are written to CSV.

Heavy work is done in Neo4j (Cypher + GDS WCC). Python is used only for
driver setup, running queries, collecting results, building DataFrames, and writing CSV.
"""

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from neo4j import GraphDatabase


class GraphFeatures:
    def __init__(self, secrets: dict, hours_window: int = 24):
        self.driver = GraphDatabase.driver(**secrets["write_neo4j"])
        self.hours_window = hours_window

    def close(self) -> None:
        self.driver.close()

    def _cutoff(self) -> datetime:
        return datetime.now(timezone.utc) - timedelta(hours=self.hours_window)

    def _get_nodes_with_degree(self) -> pd.DataFrame:
        """Return nodes ingested in the last hours_window with degree centrality (Neo4j)."""
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
                   n.value AS value,
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
                    "value",
                    "request_timestamp",
                    "degree",
                ]
            )
        return pd.DataFrame(rows)

    def _get_wcc_and_label_components(self):
        """
        Run GDS Weakly Connected Components on the recent subgraph, then for each node
        return (nodeId, componentId, label) so Python can merge component_id and
        compute component counts by type. All graph processing in Neo4j.
        Returns list of dicts with keys: nodeId, componentId, label (label may be null).
        """
        cutoff = self._cutoff()
        # GDS Cypher projection may not see outer $cutoff; pass as literal for the inner queries
        cutoff_str = cutoff.isoformat()
        node_query = "MATCH (n) WHERE n.requestTimestamp IS NOT NULL AND n.requestTimestamp >= datetime($cutoff) RETURN id(n) AS id"
        rel_query = (
            "MATCH (a)-[r]-(b) "
            "WHERE (a.requestTimestamp IS NOT NULL AND a.requestTimestamp >= datetime($cutoff)) "
            "   OR (b.requestTimestamp IS NOT NULL AND b.requestTimestamp >= datetime($cutoff)) "
            "RETURN id(a) AS source, id(b) AS target"
        )
        # Single Neo4j query: WCC stream then join to node labels, one row per (node, label)
        query = """
            CALL gds.wcc.stream({
                nodeQuery: $nodeQuery,
                relationshipQuery: $relQuery,
                parameters: { cutoff: $cutoff }
            })
            YIELD nodeId, componentId
            MATCH (n) WHERE id(n) = nodeId
            WITH nodeId, componentId, labels(n) AS labels
            UNWIND CASE WHEN labels IS NOT NULL AND size(labels) > 0 THEN labels ELSE [null] END AS label
            RETURN nodeId, componentId, label
        """
        params = {
            "cutoff": cutoff_str,
            "nodeQuery": node_query,
            "relQuery": rel_query,
        }
        with self.driver.session() as session:
            result = session.run(query, params)
            return [dict(record) for record in result]

    def _get_aggregated_stats_from_neo4j(self) -> dict:
        """
        One Neo4j transaction: degree stats (nodes, mean, std, min, max, median) and
        edge count for the recent window. Returns a single dict of aggregates.
        """
        cutoff = self._cutoff()
        degree_query = """
            MATCH (n)
            WHERE n.requestTimestamp IS NOT NULL AND n.requestTimestamp >= $cutoff
            OPTIONAL MATCH (n)-[r]-()
            WITH n, count(r) AS degree
            RETURN count(n) AS nodes_in_window,
                   avg(degree) AS degree_mean,
                   stDev(degree) AS degree_std,
                   min(degree) AS degree_min,
                   max(degree) AS degree_max,
                   percentileCont(degree, 0.5) AS degree_median
        """
        edge_query = """
            MATCH (a)-[r]-(b)
            WHERE (a.requestTimestamp IS NOT NULL AND a.requestTimestamp >= $cutoff)
               OR (b.requestTimestamp IS NOT NULL AND b.requestTimestamp >= $cutoff)
            RETURN count(r) / 2 AS edges_in_window
        """
        with self.driver.session() as session:
            deg_result = session.run(degree_query, cutoff=cutoff)
            deg_record = deg_result.single() or {}
            edge_result = session.run(edge_query, cutoff=cutoff)
            edge_record = edge_result.single() or {}
        return {
            "nodes_in_window": deg_record.get("nodes_in_window") or 0,
            "degree_mean": float(deg_record.get("degree_mean") or 0.0),
            "degree_std": float(deg_record.get("degree_std") or 0.0),
            "degree_min": int(deg_record.get("degree_min") or 0),
            "degree_max": int(deg_record.get("degree_max") or 0),
            "degree_median": float(deg_record.get("degree_median") or 0.0),
            "edges_in_window": int(edge_record.get("edges_in_window") or 0),
        }

    def _wcc_fallback_python(self, edges: List[Tuple[int, int]]) -> Dict[int, int]:
        """Union-Find in Python when GDS is not available. Returns node_id -> component_id."""
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
        return {node: find(node) for node in parent}

    def _get_edges_for_recent_nodes(self) -> List[Tuple[int, int]]:
        """Return edges where at least one endpoint was ingested in the window (Neo4j)."""
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

    def extract_features(
        self,
    ) -> Tuple[pd.DataFrame, Optional[List[dict]]]:
        """
        Extract node-level features for nodes ingested in the last 24 hours:
        node_id, labels, hashed_value, request_timestamp, degree, component_id.
        Uses Neo4j for degree and GDS WCC for components; falls back to Python
        union-find if GDS is unavailable.
        Returns (node_df, wcc_rows). wcc_rows is None if no nodes.
        """
        node_df = self._get_nodes_with_degree()
        if node_df.empty:
            node_df["component_id"] = pd.Series(dtype="Int64")
            return node_df, None

        wcc_rows = None
        try:
            wcc_rows = self._get_wcc_and_label_components()
        except Exception:
            edges = self._get_edges_for_recent_nodes()
            comp = self._wcc_fallback_python(edges)
            node_df["component_id"] = node_df["node_id"].map(
                lambda nid: comp.get(int(nid), int(nid))
            )
            return node_df, None

        comp_map = {r["nodeId"]: r["componentId"] for r in wcc_rows}
        node_df["component_id"] = node_df["node_id"].map(
            lambda nid: comp_map.get(int(nid), int(nid))
        )
        return node_df, wcc_rows

    def compute_merchant_aggregated_features(
        self, df: pd.DataFrame, wcc_rows: Optional[List[dict]] = None
    ) -> pd.DataFrame:
        """
        Aggregate into one row per run (merchant-level summary). Degree and edge
        stats come from Neo4j; unique_components and components_by_label from
        Neo4j (GDS + Cypher) or from wcc_rows when provided.
        """
        if df.empty:
            return self._empty_aggregated_row()

        now = datetime.now(timezone.utc)
        agg = {
            "feature_date": now.date().isoformat(),
            "feature_run_utc": now.isoformat(),
        }

        stats = self._get_aggregated_stats_from_neo4j()
        agg["nodes_in_window"] = stats["nodes_in_window"]
        agg["edges_in_window"] = stats["edges_in_window"]
        agg["degree_mean"] = stats["degree_mean"]
        agg["degree_std"] = stats["degree_std"]
        agg["degree_min"] = stats["degree_min"]
        agg["degree_max"] = stats["degree_max"]
        agg["degree_median"] = stats["degree_median"]

        if wcc_rows is not None:
            unique_comps = len({r["componentId"] for r in wcc_rows})
            # One row per (node, label); count distinct componentId per label
            seen: Dict[str, set] = defaultdict(set)
            for r in wcc_rows:
                if r.get("label") is not None:
                    seen[r["label"]].add(r["componentId"])
            for label, comp_ids in seen.items():
                agg[f"components_with_{label}"] = len(comp_ids)
        else:
            unique_comps = (
                int(df["component_id"].nunique()) if "component_id" in df.columns else 0
            )
            # Fallback: compute component counts by type from node-level df
            seen = defaultdict(set)
            for _, row in df.iterrows():
                cid = row.get("component_id")
                if pd.isna(cid):
                    continue
                for label in row.get("labels") or []:
                    seen[label].add(cid)
            for label, comp_ids in seen.items():
                agg[f"components_with_{label}"] = len(comp_ids)

        agg["unique_components"] = unique_comps
        return pd.DataFrame([agg])

    def _empty_aggregated_row(self) -> pd.DataFrame:
        now = datetime.now(timezone.utc)
        return pd.DataFrame(
            [
                {
                    "feature_date": now.date().isoformat(),
                    "feature_run_utc": now.isoformat(),
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
        """
        node_df, wcc_rows = self.extract_features()
        agg_df = self.compute_merchant_aggregated_features(node_df, wcc_rows=wcc_rows)

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
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
