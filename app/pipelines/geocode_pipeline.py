import time

from app.external.maps_client import geocode_address
from app.repositories.greenhouse_repo import (
    fetch_missing_batch,
    get_from_cache,
    increment_attempt,
    insert_into_cache,
)
from app.services.geocode_service import prepare_address, should_retry


def insert_geocoded_record(connection, record, lat, lon):
    """
    Insert or update a geocoded greenhouse record in the database.

    Parameters
    ----------
    connection : Any
        Database connection.
    record : Dict
        Greenhouse record.
    lat : float
        Latitude value.
    lon : float
        Longitude value.

    Returns
    -------
    None
    """
    cursor = connection.cursor()

    cursor.execute(
        """
        INSERT INTO greenhouses
        (id, name, farmer_name, phone, latitude, longitude, status, geocoded)
        VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
        ON CONFLICT(id) DO UPDATE SET
            latitude=EXCLUDED.latitude,
            longitude=EXCLUDED.longitude,
            geocoded=True
        """,
        (
            record["id"],
            record["name"],
            record["farmer_name"],
            record["phone"],
            lat,
            lon,
            record["status"],
        ),
    )

    connection.commit()
    cursor.close()


def delete_from_missing(connection, record_id):
    """
    Remove a record from the missing location table.

    Parameters
    ----------
    connection : Any
        Database connection.
    record_id : str
        ID of the record to delete.

    Returns
    -------
    None
    """
    cursor = connection.cursor()

    cursor.execute(
        "DELETE FROM greenhouses_missing_location WHERE id = %s",
        (record_id,),
    )

    connection.commit()
    cursor.close()


def run_geocode_pipeline(connection, batch_size: int = 100):
    """
    Execute geocoding pipeline for records missing location data.

    Workflow:
    - Fetch batch of records
    - Build address
    - Check retry eligibility
    - Use cache or call API
    - Store results
    - Remove processed records

    Parameters
    ----------
    connection : Any
        Database connection.
    batch_size : int, optional
        Number of records per batch.

    Returns
    -------
    None
    """
    total_processed = 0

    while True:
        records = fetch_missing_batch(connection, batch_size)

        if not records:
            print("No more records to process.")
            break

        print(f"Processing batch of {len(records)} records...")

        for record in records:
            processed = process_record(connection, record)

            if processed:
                total_processed += 1

            time.sleep(0.05)

    print(f"Total processed: {total_processed}")


def handle_failed_geocode(connection, record_id):
    """
    Handle a failed geocoding attempt for a record.

    This function updates retry attempt count for the record
    to prevent infinite retries.

    Parameters
    ----------
    connection : Any
        Database connection object.
    record_id : str
        Unique identifier of the greenhouse record.

    Returns
    -------
    None
    """
    increment_attempt(connection, record_id)


def get_coordinates(connection, address, record_id):
    """
    Resolve geographic coordinates using cache or external API.

    Parameters
    ----------
    connection : Any
        Database connection object.
    address : str
        Address string to geocode.
    record_id : str
        Unique identifier of the greenhouse record.

    Returns
    -------
    Dict[str, Any]
        Dictionary containing:
        - success : bool
        - latitude : float (if success)
        - longitude : float (if success)
    """
    cached = get_from_cache(connection, address)

    if cached:
        print(f"Cache hit: {address}")
        return cached

    try:
        print(f"Calling API: {address}")
        lat, lon = geocode_address(address)
        insert_into_cache(connection, address, lat, lon)
        return lat, lon

    except ValueError:
        print(f"Geocode failed (invalid address): {record_id}")
        handle_failed_geocode(connection, record_id)
        return None

    except RuntimeError as e:
        print(f"API error: {e}")
        return None


def process_record(connection, record):
    """
    Process a single greenhouse record through the geocoding workflow.

    This function orchestrates the geocoding steps:
    - Build address from record
    - Validate retry eligibility
    - Resolve coordinates (cache/API)
    - Persist results and cleanup

    Parameters
    ----------
    connection : Any
        Database connection object.
    record : Dict
        Greenhouse record containing address and metadata.

    Returns
    -------
    bool
        True if record was successfully geocoded and stored, False otherwise.
    """
    try:
        record_id = record.get("id")
        print(f"Processing ID: {record_id}")

        address = prepare_address(record)
        if not address:
            return False

        if not should_retry(record.get("attempts", 0)):
            return False

        coords = get_coordinates(connection, address, record_id)
        if not coords:
            return False

        lat, lon = coords

        persist_geocoded_result(connection, record, lat, lon)

        return True

    except Exception as e:
        print(f"Error processing record {record.get('id')}: {e}")
        return False


def persist_geocoded_result(connection, record, lat, lon):
    """
    Persist geocoded coordinates and remove record from pending queue.

    This function inserts or updates the greenhouse record with
    resolved coordinates and removes it from the missing location table.

    Parameters
    ----------
    connection : Any
        Database connection object.
    record : Dict
        Greenhouse record.
    lat : float
        Latitude value.
    lon : float
        Longitude value.

    Returns
    -------
    None
    """
    record_id = record["id"]

    insert_geocoded_record(connection, record, lat, lon)
    print(f"Inserted: {record_id}")

    delete_from_missing(connection, record_id)
    print(f"Deleted: {record_id}")
