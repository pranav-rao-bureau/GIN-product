"""
This module contains all functions to extract features from the merchant neo4j graph instance.
Functions include:
- node_centrality
- connected_component_count (by node type)
- Additional network based features
"""

import pandas as pd


class GraphFeatures:
    def __init__(self, secrets: dict):
        self.secrets = secrets

    def extract_features(self) -> pd.DataFrame:
        pass

    def compute_merchant_aggregated_features(self, df) -> pd.DataFrame:
        pass
