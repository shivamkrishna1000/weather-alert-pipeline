from app.constants import ALLOWED_STATUSES, ZOHO_FIELDS
from app.core.greenhouse import extract_fields, filter_greenhouses, split_records


def process_greenhouse_records(
    records: list[dict[str, object]],
) -> tuple[list[dict], list[dict]]:
    """
    Process raw greenhouse records into cleaned datasets.

    Workflow:
    - Split records into those with and without location data
    - Filter both groups based on allowed statuses
    - Extract structured fields for records with valid location

    Parameters
    ----------
    records : list[dict]
        Raw greenhouse records from Zoho.

    Returns
    -------
    tuple[list[dict], list[dict]]
        - Cleaned records with valid location (structured format)
        - Records without location (filtered but not transformed)
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


def filter_records_without_location(
    records: list[dict], allowed_statuses: set, fields: dict
) -> list[dict]:
    """
    Filter records without location based on allowed statuses.

    This function prepares records for the geocoding pipeline
    by retaining only those with valid operational status.

    Parameters
    ----------
    records : list[dict]
        Records missing latitude/longitude.
    allowed_statuses : set
        Valid greenhouse statuses.
    fields : dict
        Mapping of field names.

    Returns
    -------
    list[dict]
        Filtered records eligible for geocoding.
    """
    filtered = []

    for record in records:
        status = record.get(fields["status"])

        if status in allowed_statuses:
            filtered.append(record)

    return filtered
