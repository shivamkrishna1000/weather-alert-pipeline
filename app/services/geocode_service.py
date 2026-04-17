from typing import Dict, Optional

from app.core.geocode import build_address


def prepare_address(record: Dict) -> Optional[str]:
    """
    Construct a normalized address string from a greenhouse record.

    This is a thin wrapper over `build_address` used to standardize
    address preparation before geocoding.

    Parameters
    ----------
    record : dict
        Greenhouse record containing address components.

    Returns
    -------
    str or None
        Constructed address string, or None if insufficient data is present.
    """
    return build_address(record)


def should_retry(attempts: int) -> bool:
    """
    Determine if a record is eligible for another geocoding attempt.

    Parameters
    ----------
    attempts : int
        Number of previous geocoding attempts.

    Returns
    -------
    bool
        True if attempts are below retry limit (currently 3), else False.
    """
    return attempts < 3
