import os
from typing import Dict, List

import pandas as pd
from pyathena import connect
import yaml

from utils.logs_processing import EventTypes, pan_profile_processing


PROCESSING_FUNCTIONS = {
    EventTypes.PAN_PROFILE.value: pan_profile_processing,
    EventTypes.PAN_ADVANCED.value: pan_profile_processing,
    # add event types are processing functions are implemented here
}


def load_config(config_path: str) -> dict:
    """Load merchant/config from a YAML file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def load_secrets(secrets_file: str = "secrets.yml") -> dict:
    """Load secrets from a YAML file."""
    if not os.path.exists(secrets_file):
        raise FileNotFoundError(f"Secrets file not found: {secrets_file}")
    with open(secrets_file, "r") as f:
        secrets = yaml.load(f, Loader=yaml.FullLoader)
    return secrets


def athena_connect(secrets):
    """
    Uses boto3 and a config/secrets_file to create an Athena client object.
    """
    athena_cfg = secrets["athena"]
    athena_client = connect(**athena_cfg)
    return athena_client


def retrieve_api_logs(
    athena_client,
    merchant_id: str,
    end_date: str,
    start_date: str = None,
    data_retention_days: int = None,
    event_types: List[str] = None,
):
    """
    Query all identities for a merchant_id for a given time frame from the API call logs.
    """

    if start_date is None and data_retention_days is None:
        raise Exception("Please provide either start date or data retention days")

    if start_date is None:
        start_date = end_date - data_retention_days

    query = f"""
    SELECT merchantid,
        event, 
        merchantrequestbody,
        response,
        requesttimestamp,
    FROM base.service_api_logevent_pqt
    where merchantid = {merchant_id}
    // make use of partition fields (year month day and hour) to filter the logs 
    // rather than using requesttimestamp between {start_date} and {end_date}
    and requesttimestamp between {start_date} and {end_date}
    and merchantstatuscode = 200
    and event in ({','.join(event_types)})
    """

    return athena_client.query(query).to_pandas()


def extract_ids_from_logs(
    logs: pd.DataFrame, event_types: List[str]
) -> Dict[str, pd.DataFrame]:
    """
    Extract the IDs from the API logs for a list of event types.
    Args:
        logs: pd.DataFrame containing the API logs.
        event_types: List[str] containing the event types.
        processing_function: callable function to process the logs.
    Returns:
        dataframe containing the IDs.
    """
    results = {}
    for event_type in event_types:
        processing_function = PROCESSING_FUNCTIONS[event_type]
        result = processing_function(logs[logs["event"] == event_type])
        results[event_type] = result
    return results
