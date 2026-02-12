from enum import Enum
import json

import pandas as pd


class IdentityTypes(Enum):
    PAN = "pan"
    NAME = "fullName"
    DL = "dlNumber"
    RC = "vehicleRC"
    PHONE = "mobile"
    EMAIL = "email"
    VOTER = "voterId"
    ADDRESS = "completeAddress"


class EventTypes(Enum):
    PAN_PROFILE = "pan-profile"
    PAN_ADVANCED = "pan-advanced"
    DL = "dummy_dl"
    RC = "dummy_rc"
    PHONE = "dummy_phone"
    EMAIL = "dummy_email"
    VOTER = "dummy_voter"
    ADDRESS = "dummy_address"


def pan_profile_processing(logs: pd.DataFrame) -> pd.DataFrame:
    """
    Process the PAN profile logs.

    """
    result = pd.DataFrame()
    result[IdentityTypes.PAN.value] = logs.merchantrequestbody.apply(
        lambda x: json.loads(x).get("pan").lower()
    )
    result["requestTimestamp"] = logs.requesttimestamp
    result.sort_values(by="requestTimestamp", inplace=True)
    return result.drop_duplicates(subset=[IdentityTypes.PAN.value], keep="last")


def dl_processing(logs: pd.DataFrame) -> pd.DataFrame:
    pass


def phone_processing(logs: pd.DataFrame) -> pd.DataFrame:
    pass


def email_processing(logs: pd.DataFrame) -> pd.DataFrame:
    pass


def rc_processing(logs: pd.DataFrame) -> pd.DataFrame:
    pass
