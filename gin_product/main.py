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
        type=str,
        default="conf.yml",
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
        default=date.today(),
        type=lambda d: date.fromisoformat(d) if isinstance(d, str) else d,
        help="End date for API logs extraction in 'YYYY-MM-DD' format",
    )
    parser.add_argument(
        "--start-date",
        required=False,
        default=(date.today() - timedelta(days=1)),
        type=lambda d: date.fromisoformat(d) if isinstance(d, str) else d,
        help="Start date for API logs extraction in 'YYYY-MM-DD' format (as datetime.date object)",
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
        action="store_true",
        help="Whether to clear the graph before setup",
    )
    return parser


import logging


def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger = logging.getLogger("gin_product.main")

    parser = create_parser()
    args = parser.parse_args()

    logger.info("Loading config file: %s", args.config)
    config = load_config(args.config)
    merchant_config = config.get("merchant_config", {})
    merchant_id = merchant_config.get("merchantid")
    logger.info("Merchant ID loaded: %s", merchant_id)

    logger.info("Loading secrets file: %s", args.secrets)
    secrets = load_secrets(args.secrets)

    # Extract IDs from API logs using merchant config
    end_date = args.end_date
    start_date = args.start_date
    logger.info(
        "Connecting to Athena and extracting API logs for merchant '%s' from %s to %s with event types: %s",
        merchant_id,
        start_date,
        end_date,
        args.event_types,
    )

    athena_client = athena_connect(secrets)
    logs = retrieve_api_logs(
        athena_client,
        merchant_id=merchant_id,
        end_date=end_date,
        start_date=start_date,
        event_types=args.event_types,
    )
    logger.info("Extracted %d API logs", len(logs))

    logger.info("Extracting IDs from logs with event types: %s", args.event_types)
    seeds: Dict[str, pd.DataFrame] = extract_ids_from_logs(logs, args.event_types)

    logger.info("Extracting subgraphs with terminators...")
    extractor = SubgraphExtractor(secrets, batch_size=args.batch_size, logger=logger)
    try:
        subgraph = extractor.extract_subgraph_with_terminators(seeds=seeds)
        logger.info("Subgraph extraction completed")
    finally:
        extractor.close()
        logger.info("Closed SubgraphExtractor connection")
    logger.info(
        "Extracted subgraphs with %d nodes and %d relationships",
        len(subgraph["nodes"]),
        len(subgraph["relationships"]),
    )

    logger.info("Initializing graph (clear graph: %s)", args.clear_graph)
    initializer = GraphInitializer(
        secrets, logger=logger, do_clear_graph=args.clear_graph
    )
    try:
        initializer.setup()
        logger.info("GraphInitializer setup completed")
    finally:
        initializer.close()
        logger.info("Closed GraphInitializer connection")

    logger.info(
        "Writing subgraphs to graph DB with %d nodes and %d relationships",
        len(subgraph["nodes"]),
        len(subgraph["relationships"]),
    )

    writer = SubgraphWriter(secrets, logger=logger, batch_size=args.batch_size)
    try:
        writer.write_subgraphs(subgraph, seed_identities=seeds)
        logger.info(
            "Subgraphs with %d nodes and %d relationships written to graph DB successfully",
            len(subgraph["nodes"]),
            len(subgraph["relationships"]),
        )
    finally:
        writer.close()
        logger.info("Closed SubgraphWriter connection")


if __name__ == "__main__":
    main()
