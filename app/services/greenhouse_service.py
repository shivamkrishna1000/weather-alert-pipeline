from typing import Any, Dict, List, Tuple

from app.constants import ALLOWED_STATUSES, ZOHO_FIELDS
from app.core.greenhouse import extract_fields, filter_greenhouses, split_records


def process_greenhouse_records(
    records: List[Dict[str, Any]],
) -> Tuple[List[Dict], List[Dict]]:
    """
    Process raw Zoho greenhouse records into cleaned datasets.

    Parameters
    ----------
    records : List[Dict[str, Any]]
        Raw greenhouse records from Zoho.

    Returns
    -------
    Tuple[List[Dict], List[Dict]]
        - Cleaned records with valid location
        - Records missing location data
    """

    with_loc, without_loc = split_records(records)

    with_loc = filter_greenhouses(
        records=with_loc,
        allowed_statuses=ALLOWED_STATUSES,
        fields=ZOHO_FIELDS,
    )

    without_loc = without_loc = filter_records_without_location(
        records=without_loc,
        allowed_statuses=ALLOWED_STATUSES,
        fields=ZOHO_FIELDS,
    )

    cleaned = extract_fields(
        records=with_loc,
        fields=ZOHO_FIELDS,
    )

    return cleaned, without_loc


def filter_records_without_location(records, allowed_statuses, fields):
    """
    Filter greenhouse records without location based on allowed statuses.

    Parameters
    ----------
    records : List[Dict[str, Any]]
        List of greenhouse records without coordinates.
    allowed_statuses : set
        Set of valid greenhouse statuses.
    fields : Dict
        Mapping of field names from Zoho schema.

    Returns
    -------
    List[Dict[str, Any]]
        Filtered records matching allowed statuses.
    """
    filtered = []

    for record in records:
        status = record.get(fields["status"])

        if status in allowed_statuses:
            filtered.append(record)

    return filtered
