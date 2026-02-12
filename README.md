# Neo4j Graph Feature Toolkit

This project provides a set of Python modules for interacting with and analyzing a merchant-oriented Neo4j graph database. It is designed for advanced feature engineering, data cleansing, and subgraph management, with a focus on processing node identities.

## Features

- !!TODO **Genericness Calculation**:  
  Methods to compute the genericness of nodes (such as emails or phone numbers), which assist in validating identities and cleaning the graph.
 
- **Graph Feature Extraction**:  
  Tools to extract structural and statistical features from the graph, including node centrality metrics and connected component analysis.

- **Subgraph Management via CLI**:  
  A command-line interface for retrieving, processing, and exporting subgraphs based on seed identities, supporting common data science workflows.

## Code Structure

- `main.py`  
  The entrypoint script for CLI-based workflows. Handles argument parsing, configuration, and orchestrates subgraph operations.

- `utils/genericness.py`  
  Contains functions for computing the genericness of nodes by identity type, considering both node value and connectivity.

- `utils/graph_features.py`  
  Provides functions for extracting graph features such as centrality and connected component counts.

- `conf/conf.yml`  
  Configuration file with connection and parameter details for interacting with data sources.

## Getting Started

1. **Set up the environment**  
   - Clone the repository.
   - Install dependencies:  
     ```
     python -m venv .venv
     source .venv/bin/activate
     pip install -r requirements.txt
     ```

2. **Configure connection**  
   - Edit `conf/conf.yml` with your Neo4j connection info and relevant parameters.

3. **Run subgraph extraction**  
   - Use the CLI:  
     ```
     python main.py conf/conf.yml
     ```

## Usage Examples

- **Calculating node genericness**  
  Use the functions in `utils/genericness.py` to obtain confidence scores for node authenticity.

- **Graph Feature Extraction**  
  Call the routines in `utils/graph_features.py` for advanced graph analytics.

## Development Notes

- Python 3.11+ required.
- See `.gitignore` for development environment suggestions.
- Extend feature extraction by adding new functions to `utils/graph_features.py`.
