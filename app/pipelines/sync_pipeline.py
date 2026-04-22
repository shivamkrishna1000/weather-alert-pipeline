from datetime import datetime, timedelta

from app.constants import ALLOWED_STATUSES, ZOHO_FIELDS
from app.database import create_tables, update_last_sync_time
from app.external.zoho_client import fetch_all_greenhouse_data
from app.repositories.greenhouse_repo import (
    delete_greenhouse,
    insert_greenhouses,
    insert_missing_location,
)
from app.services.greenhouse_service import process_greenhouse_records


def run_sync_pipeline(connection):
    """
    Execute greenhouse data synchronization pipeline.

    This function orchestrates the full sync workflow:
    - Ensure tables exist
    - Fetch raw data from Zoho
    - Separate valid and invalid records
    - Delete invalid records
    - Process and store valid records
    - Update sync timestamp

    Parameters
    ----------
    connection : Any
        Database connection.

    Returns
    -------
    None
    """
    try:
        create_tables(connection)

        raw_records = fetch_all_greenhouse_data(connection)

        if not raw_records:
            print("No records fetched.")
            return

        valid_records, invalid_ids = separate_valid_invalid_records(raw_records)

        delete_invalid_greenhouses(connection, invalid_ids)

        cleaned_with_loc, without_loc = process_greenhouse_records(valid_records)

        insert_greenhouses(connection, cleaned_with_loc)
        insert_missing_location(connection, without_loc)

        update_sync_timestamp(connection, raw_records)

        print("Sync pipeline completed.")

    except (RuntimeError, ValueError) as e:
        print(f"Sync pipeline failed: {e}")
        raise


def separate_valid_invalid_records(records: list[dict]) -> tuple[list[dict], list[str]]:
    """
    Separate greenhouse records into valid and invalid sets.

    Valid records are those with allowed status values.
    Invalid records are identified by their IDs for deletion.

    Parameters
    ----------
    records : list[dict]
        Raw greenhouse records from Zoho.

    Returns
    -------
    tuple[list[dict], list[str]]
        - Valid records
        - List of invalid record IDs
    """
    valid = []
    invalid_ids = []

    for record in records:
        status = record.get(ZOHO_FIELDS["status"])
        record_id = record.get(ZOHO_FIELDS["id"])

        if status in ALLOWED_STATUSES:
            valid.append(record)
        else:
            invalid_ids.append(record_id)

    return valid, invalid_ids


def delete_invalid_greenhouses(connection, invalid_ids: list[str]) -> None:
    """
    Delete greenhouse records that are no longer valid.

    Parameters
    ----------
    connection : Any
        Database connection.
    invalid_ids : list[str]
        List of greenhouse IDs to delete.

    Returns
    -------
    None
    """
    for gid in invalid_ids:
        delete_greenhouse(connection, gid)


def update_sync_timestamp(connection, records: list[dict]) -> None:
    """
    Update last synchronization timestamp based on fetched records.

    Parameters
    ----------
    connection : Any
        Database connection.
    records : list[dict]
        Raw greenhouse records.

    Returns
    -------
    None
    """
    timestamps = [r.get("Modified_Time") for r in records if r.get("Modified_Time")]

    if not timestamps:
        print("No valid Modified_Time found. Skipping sync timestamp update.")
        return

    latest_time = max(timestamps)
    latest_dt = datetime.fromisoformat(latest_time) + timedelta(seconds=1)

    update_last_sync_time(connection, latest_dt.isoformat())
