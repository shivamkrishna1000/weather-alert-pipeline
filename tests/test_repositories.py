import os

from app.config import load_environment
from app.database import create_tables, get_connection
from app.repositories.greenhouse_repo import (
    fetch_missing_batch,
    get_from_cache,
    increment_attempt,
    insert_greenhouses,
    insert_into_cache,
)


def get_test_db():
    load_environment()
    database_url = os.environ.get("TEST_DATABASE_URL")
    return get_connection(database_url)


def clean_tables(connection):
    cursor = connection.cursor()

    cursor.execute("DELETE FROM greenhouses")
    cursor.execute("DELETE FROM geocode_cache")
    cursor.execute("DELETE FROM greenhouses_missing_location")

    connection.commit()


# ------------------ TESTS ------------------


def test_insert_and_fetch_greenhouses():
    connection = get_test_db()
    create_tables(connection)
    clean_tables(connection)

    records = [
        {
            "id": "1",
            "greenhouse_name": "GH1",
            "farmer_name": "Ravi",
            "phone": "999",
            "latitude": 10.0,
            "longitude": 20.0,
            "status": "active",
        }
    ]

    insert_greenhouses(connection, records)

    cursor = connection.cursor()
    cursor.execute("SELECT id FROM greenhouses")
    rows = cursor.fetchall()

    assert rows[0][0] == "1"

    connection.close()


def test_cache_insert_and_fetch():
    connection = get_test_db()
    create_tables(connection)
    clean_tables(connection)

    insert_into_cache(connection, "addr", 1.0, 2.0)

    result = get_from_cache(connection, "addr")

    assert result == (1.0, 2.0)

    connection.close()


def test_increment_attempt():
    connection = get_test_db()
    create_tables(connection)
    clean_tables(connection)

    cursor = connection.cursor()
    cursor.execute(
        "INSERT INTO greenhouses_missing_location (id, attempts) VALUES (%s, %s)",
        ("1", 0),
    )
    connection.commit()

    increment_attempt(connection, "1")

    cursor.execute(
        "SELECT attempts FROM greenhouses_missing_location WHERE id=%s",
        ("1",),
    )

    assert cursor.fetchone()[0] == 1

    connection.close()


def test_fetch_missing_batch():
    connection = get_test_db()
    create_tables(connection)
    clean_tables(connection)

    cursor = connection.cursor()
    cursor.execute(
        "INSERT INTO greenhouses_missing_location (id, attempts) VALUES (%s, %s)",
        ("1", 0),
    )
    connection.commit()

    result = fetch_missing_batch(connection, limit=10)

    assert len(result) == 1

    connection.close()
