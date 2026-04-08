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
    Execute full greenhouse data synchronization pipeline.

    Steps include:
    - Ensuring database schema exists
    - Fetching data from Zoho
    - Processing records
    - Storing cleaned data
    - Updating last sync timestamp

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

        valid_raw_records = []
        invalid_ids = []

        # Step 1: separate valid & invalid
        for record in raw_records:
            status = record.get(ZOHO_FIELDS["status"])
            record_id = record.get(ZOHO_FIELDS["id"])

            if status in ALLOWED_STATUSES:
                valid_raw_records.append(record)
            else:
                invalid_ids.append(record_id)

        # Step 2: delete invalid
        for gid in invalid_ids:
            delete_greenhouse(connection, gid)

        # Step 3: process only valid records
        cleaned_with_loc, without_loc = process_greenhouse_records(valid_raw_records)

        # Step 4: insert/update
        insert_greenhouses(connection, cleaned_with_loc)
        insert_missing_location(connection, without_loc)

        timestamps = [
            r.get("Modified_Time") for r in raw_records if r.get("Modified_Time")
        ]

        if not timestamps:
            print("No valid Modified_Time found. Skipping sync timestamp update.")
            return

        latest_time = max(timestamps)

        latest_dt = datetime.fromisoformat(latest_time) + timedelta(seconds=1)

        update_last_sync_time(connection, latest_dt.isoformat())

        print("Sync pipeline completed.")

    except (RuntimeError, ValueError) as e:
        print(f"Sync pipeline failed: {e}")
        raise
