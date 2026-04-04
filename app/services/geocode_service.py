from typing import Dict, Optional

from app.core.geocode import build_address


def prepare_address(record: Dict) -> Optional[str]:
    """
    Prepare address string from greenhouse record.

    Parameters
    ----------
    record : Dict
        Greenhouse record.

    Returns
    -------
    Optional[str]
        Constructed address string or None.
    """
    return build_address(record)


def should_retry(attempts: int) -> bool:
    """
    Determine whether geocoding should be retried for a record.

    Parameters
    ----------
    attempts : int
        Number of previous attempts.

    Returns
    -------
    bool
        True if retry is allowed, False otherwise.
    """
    return attempts < 3
