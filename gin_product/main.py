#!/usr/bin/env python3
"""
Main entry point: load config and secrets, extract IDs from merchant logs,
extract subgraphs from Neo4j, and write them to the target Neo4j instance.
"""
import argparse
from datetime import date, timedelta
from typing import Dict

import pandas as pd

from gin_product.utils.graph_utils import (
    GraphInitializer,
    SubgraphExtractor,
    SubgraphWriter,
)
from gin_product.utils.logs_processing import EventTypes
from gin_product.utils.utils import (
    athena_connect,
    extract_ids_from_logs,
    load_config,
    load_secrets,
    retrieve_api_logs,
)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract merchant IDs from service logs, subgraphs from Global GIN, and write to Merchant GIN.",
        epilog="Example: python main.py --secrets ./secrets.yml --config ./conf/conf.yml --event-types pan-profile --end-date 2026-02-06 --start-date 2026-02-05 --batch-size 1000",
        formatter_class=argparse.MetavarTypeHelpFormatter,
    )
    parser.add_argument(
        "--secrets",
        required=True,
        type=str,
        default="./secrets.yml",
        help="Path to the secrets YAML file (e.g. ./secrets.yml)",
    )
    parser.add_argument(
        "--config",
        required=True,
        type=str,
        default="./conf/conf.yml",
        help="Path to the merchant config YAML file (e.g. ./conf/conf.yml)",
    )
    parser.add_argument(
        "--event-types",
        nargs="+",
        required=True,
        type=str,
        help=f"Event types for ID extraction from logs (e.g. {', '.join([event_type.value for event_type in EventTypes][:2])})",
    )

    parser.add_argument(
        "--end-date",
        required=False,
        default=date.today().isoformat(),
        type=str,
        help="End date for API logs extraction in 'YYYY-MM-DD' format",
    )
    parser.add_argument(
        "--start-date",
        required=False,
        default=(date.today() - timedelta(days=1)).isoformat(),
        type=str,
        help="Start date for API logs extraction in 'YYYY-MM-DD' format",
    )
    parser.add_argument(
        "--batch-size",
        required=False,
        default=1000,
        type=int,
        help="Batch size for processing node values",
    )
    parser.add_argument(
        "--clear-graph",
        required=False,
        default=False,
        type=bool,
        help="Whether to clear the graph before setup",
    )
    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()

    config = load_config(args.config)
    merchant_config = config.get("merchant_config", {})
    merchant_id = merchant_config.get("merchantid")

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

    extractor = SubgraphExtractor(secrets, batch_size=args.batch_size)
    subgraph = extractor.extract_subgraph_with_terminators(seeds=seeds)
    extractor.close()

    initializer = GraphInitializer(secrets, do_clear_graph=args.clear_graph)
    initializer.setup()
    initializer.close()

    writer = SubgraphWriter(secrets)
    writer.write_subgraphs(subgraph, seed_identities=seeds)
    writer.close()


if __name__ == "__main__":
    main()
