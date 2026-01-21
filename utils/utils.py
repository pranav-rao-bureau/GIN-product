from typing import Dict, List


def neo4j_connect(config):
    """Returns a valid neo4j connection object based on a config"""
    pass


def athena_connect(config):
    """Returns a valid AWS Athena connection object based on a config"""
    pass


def subgraph_query(id_type: str, ids: List[str], max_depth=4) -> List[Dict]:
    """
    Query subgraphs for each seed identity upto a specified maximum depth, from global GIN.
    """
    pass


def hash_identity(id: Dict[str, str]):
    """
    Retain the id type and the relation count as well as the last seen
    Mask the value of the ID.
    """
    pass


def retrive_subgraphs(
    seed_identities: Dict[str, List[str]], max_depth=4
) -> List[Dict[str, str]]:
    """
    Fetch all subgraphs for each identity type present in the input seed_identities.

    Params:
        seed_identities: {id_type: List[ids]}
    """

    subgraphs = []
    for id_type, ids in seed_identities.items():
        subgraphs.extend(subgraph_query(id_type, ids, max_depth))

    return subgraphs


def write_subgraphs(subgraphs) -> None:
    """
    Write all the subgraphs extracted from global GIN to merchant specific GIN.
    Only seed identities have ID values, rest of the nodes are hashed.
    """

    """
    for each id in subgraphs 
    if id not in seed_identities
    apply hash_identity
    """

    pass


def retrive_identities(
    merchant_id: str,
    start_date: str = None,
    end_date: str = None,
    data_retention_days: int = None,
):
    """
    Query all identities for a merchant_id for a given time frame from the API call logs.
    """
    pass
