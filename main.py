#!/usr/bin/env python3
"""
Main entry point: load config and secrets, extract IDs from merchant logs,
extract subgraphs from Neo4j, and write them to the target Neo4j instance.
"""
import argparse
from datetime import date, timedelta
import os
from typing import Dict
import pandas as pd

import yaml

from utils.graph_utils import SubgraphExtractor, SubgraphWriter
from utils.utils import (
    athena_connect,
    extract_ids_from_logs,
    load_secrets,
    retrieve_api_logs,
)
from utils.logs_processing import EventTypes


def load_config(config_path: str) -> dict:
    """Load merchant/config from a YAML file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser(
        description="Extract merchant IDs from service logs, subgraphs from Global GIN, and write to Merchant GIN."
    )
    parser.add_argument(
        "--secrets",
        required=True,
        default="./secrets.yml",
        help="Path to the secrets YAML file (e.g. ./secrets.yml)",
    )
    parser.add_argument(
        "--config",
        required=True,
        default="./conf/conf.yml",
        help="Path to the merchant config YAML file (e.g. ./conf/conf.yml)",
    )
    parser.add_argument(
        "--event-types",
        required=True,
        choices=[
            EventTypes.PAN_PROFILE,
            EventTypes.PAN_ADVANCED,
            # add event types are processing functions are implemented
        ],
        help="Event types for ID extraction from logs",
    )

    parser.add_argument(
        "--end-date",
        required=False,
        default=date.today().isoformat(),
        help="End date for API logs extraction in 'YYYY-MM-DD' format",
    )
    parser.add_argument(
        "--start-date",
        required=False,
        default=(date.today() - timedelta(days=1)).isoformat(),
        help="Start date for API logs extraction in 'YYYY-MM-DD' format",
    )
    parser.add_argument(
        "--batch-size",
        required=False,
        default=1000,
        help="Batch size for processing node values",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    merchant_config = config.get("merchant_config", {})
    merchant_id = merchant_config.get("merchantid")
    merchant_data_retention_days = merchant_config.get(
        "merchant_data_retention_days", 14
    )

    if not merchant_id:
        raise ValueError("merchant_config.merchantid is required in config")

    secrets = load_secrets(args.secrets)

    # Extract IDs from API logs using merchant config
    end_date = args.end_date
    start_date = args.start_date
    athena_client = athena_connect(secrets)
    logs = retrieve_api_logs(
        athena_client,
        merchant_id=merchant_id,
        end_date=end_date,
        start_date=start_date,
        event_types=args.event_types,
    )
    seeds: Dict[str, pd.DataFrame] = extract_ids_from_logs(logs, args.event_types)

    # Extract subgraphs from read Neo4j
    extractor = SubgraphExtractor(secrets, batch_size=args.batch_size)
    try:
        subgraph = extractor.extract_subgraph_with_terminators(seeds=seeds)
    except Exception as e:
        print(f"Error extracting subgraphs: {e}")
        return
    finally:
        extractor.close()

    # Write subgraphs to write Neo4j
    writer = SubgraphWriter(secrets)
    try:
        writer.write_subgraphs(subgraph, seed_identities=seeds)
    except Exception as e:
        print(f"Error writing subgraphs: {e}")
        return
    finally:
        writer.close()

    print("Done: IDs extracted, subgraphs extracted and written to Neo4j.")
    print(
        f"""*-----*-----*-----*-----*-----*-----*-----*-----*-----*
    Statistics:
    Count of seeds: {len(seeds)}
    Count of nodes in subgraph: {len(subgraph["nodes"])}
    Count of relationships in subgraph: {len(subgraph["relationships"])}
    *-----*-----*-----*-----*-----*-----*-----*-----*-----*"""
    )


if __name__ == "__main__":
    main()
