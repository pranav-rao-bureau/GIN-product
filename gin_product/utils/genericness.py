"""
This module contains all functions which calculate node genericness.
Genericness algorithms will be per identity type, and will consider both;
ID value as well as node conectivity.

Genericness scores will be used to clean up nodes present on GIN as well as
provide pseudo confidence about node validity.
"""

from typing import Dict


def email_genericness(node: Dict) -> float:
    pass


def phone_genericness(node: Dict) -> float:
    pass
