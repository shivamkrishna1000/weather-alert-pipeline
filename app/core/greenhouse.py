from typing import Any, Dict, List, Tuple


def split_records(records: List[Dict[str, Any]]) -> Tuple[List[Dict], List[Dict]]:
    """
    Split greenhouse records based on availability of latitude and longitude.

    Parameters
    ----------
    records : List[Dict[str, Any]]
        List of raw greenhouse records.

    Returns
    -------
    Tuple[List[Dict], List[Dict]]
        - First list contains records with valid latitude and longitude
        - Second list contains records missing location data
    """
    with_loc = []
    without_loc = []

    for r in records:
        if r.get("Latitude") is not None and r.get("Longitude") is not None:
            with_loc.append(r)
        else:
            without_loc.append(r)

    return with_loc, without_loc


def filter_greenhouses(
    records: List[Dict[str, Any]], allowed_statuses: set, fields: Dict
) -> List[Dict[str, Any]]:
    """
    Filter greenhouse records based on allowed statuses and valid coordinates.

    Parameters
    ----------
    records : List[Dict[str, Any]]
        List of greenhouse records.
    allowed_statuses : set
        Set of valid greenhouse statuses.
    fields : Dict
        Mapping of field names from Zoho schema.

    Returns
    -------
    List[Dict[str, Any]]
        Filtered list of valid greenhouse records.
    """
    filtered = []

    for record in records:
        status = record.get(fields["status"])
        latitude = record.get(fields["latitude"])
        longitude = record.get(fields["longitude"])

        if status not in allowed_statuses:
            continue

        if latitude is None or longitude is None:
            continue

        filtered.append(record)

    return filtered


def extract_fields(records: List[Dict[str, Any]], fields: Dict) -> List[Dict[str, Any]]:
    """
    Transform raw greenhouse records into a cleaned and structured format.

    Parameters
    ----------
    records : List[Dict[str, Any]]
        List of filtered greenhouse records.
    fields : Dict
        Mapping of field names from Zoho schema.

    Returns
    -------
    List[Dict[str, Any]]
        Cleaned records with standardized keys.
    """
    cleaned = []

    for record in records:
        cleaned.append(
            {
                "greenhouse_name": record.get(fields["name"]),
                "farmer_name": (record.get(fields["farmer"]) or {}).get("name"),
                "phone": get_phone(record, fields["phone_fields"]),
                "latitude": record.get(fields["latitude"]),
                "longitude": record.get(fields["longitude"]),
                "district": record.get(fields["district"]),
                "taluk": record.get(fields["taluk"]),
                "status": record.get(fields["status"]),
                "id": record.get(fields["id"]),
            }
        )

    return cleaned


def get_phone(record, phone_fields):
    """
    Extract first valid phone number based on priority fields.

    Parameters
    ----------
    record : dict
        Greenhouse record.
    phone_fields : list
        Ordered list of phone field names.

    Returns
    -------
    str or None
    """
    for field in phone_fields:
        value = record.get(field)
        if value:
            value = value.strip().replace(" ", "")
            return value
    return None
