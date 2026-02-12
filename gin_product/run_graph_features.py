#!/usr/bin/env python3
"""
Daily cron entry point: extract graph features from the merchant Neo4j instance
(nodes ingested in the last 24 hours) and append one row to the features CSV.

Example cron (daily at 02:00 UTC):
  0 2 * * * cd /path/to/GIN-product && python run_graph_features.py --secrets ./secrets.yml --output ./output/graph_features.csv
"""
import argparse

from utils.graph_features import GraphFeatures
from utils.utils import load_secrets


def main():
    parser = argparse.ArgumentParser(
        description="Extract merchant graph features (last 24h) and append to CSV.",
    )
    parser.add_argument(
        "--secrets",
        required=True,
        help="Path to secrets YAML (must contain write_neo4j for merchant graph).",
    )
    parser.add_argument(
        "--output",
        default="./output/graph_features.csv",
        help="Path for aggregated features CSV (default: ./output/graph_features.csv).",
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=24,
        help="Time window in hours for node ingestion (default: 24).",
    )
    parser.add_argument(
        "--node-level",
        help="If set, also write node-level features to this CSV path.",
    )
    args = parser.parse_args()

    secrets = load_secrets(args.secrets)
    extractor = GraphFeatures(secrets, hours_window=args.hours)
    try:
        extractor.run_and_write_csv(
            output_path=args.output,
            include_node_level=bool(args.node_level),
            node_level_path=args.node_level,
        )
    finally:
        extractor.close()


if __name__ == "__main__":
    main()
