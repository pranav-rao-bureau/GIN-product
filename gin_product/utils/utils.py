from datetime import date, timedelta
from importlib import resources
import os
from typing import Dict, List

import pandas as pd
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
import yaml

from gin_product.utils.logs_processing import EventTypes, pan_profile_processing


PROCESSING_FUNCTIONS = {
    EventTypes.PAN_PROFILE.value: pan_profile_processing,
    EventTypes.PAN_ADVANCED.value: pan_profile_processing,
    # add event types are processing functions are implemented here
}


def load_config(config_filename: str) -> dict:
    """Load merchant/config from a YAML file."""
    with resources.files("gin_product.conf").joinpath(config_filename).open("r") as f:
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
    athena_conn = connect(**athena_cfg)
    return athena_conn


def retrieve_api_logs(
    athena_conn,
    merchant_id: str,
    end_date: date,
    start_date: date = None,
    data_retention_days: int = None,
    event_types: List[str] = None,
):
    """
    Query all identities for a merchant_id for a given time frame from the API call logs.
    """

    if start_date is None and data_retention_days is None:
        raise Exception("Please provide either start date or data retention days")

    if start_date is None:
        start_date = end_date - timedelta(days=data_retention_days)

    query = f"""
    SELECT merchantid,
        event, 
        merchantrequestbody,
        response,
        requesttimestamp
    FROM base.service_api_logevent_pqt
    WHERE merchantid = '{merchant_id}'
    AND year between {start_date.year} and {end_date.year}
    AND month between {start_date.month} and {end_date.month}
    AND day between {start_date.day} and {end_date.day}
    """

    if event_types:
        query += (
            f"""AND event in ({', '.join(f"'{event}'" for event in event_types)})"""
        )

    with athena_conn:
        cursor = athena_conn.cursor(PandasCursor)
        return cursor.execute(query).as_pandas()


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
