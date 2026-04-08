import os
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from app.config import load_environment
from app.database import create_tables, get_connection
from app.repositories.greenhouse_repo import (
    delete_greenhouse,
    fetch_missing_batch,
    get_existing_ids,
    get_from_cache,
    get_last_sync_time,
    increment_attempt,
    insert_greenhouses,
    insert_into_cache,
)
from app.repositories.weather_repo import (
    clean_name,
    fetch_clusters,
    get_cached_weather,
    insert_weather_history,
    is_cache_fresh,
    upsert_weather_cache,
)

# ------------------ DATABASE ------------------


def get_test_db():
    load_environment()
    database_url = os.environ.get("TEST_DATABASE_URL")
    return get_connection(database_url)


def clean_tables(connection):
    cursor = connection.cursor()

    cursor.execute("DROP TABLE IF EXISTS greenhouses CASCADE")
    cursor.execute("DROP TABLE IF EXISTS geocode_cache CASCADE")
    cursor.execute("DROP TABLE IF EXISTS greenhouses_missing_location CASCADE")
    cursor.execute("DROP TABLE IF EXISTS sync_metadata CASCADE")

    connection.commit()


# ------------------ GREENHOUSE REPO ------------------


def test_get_existing_ids():

    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("1",), ("2",)]

    connection = MagicMock()
    connection.cursor.return_value = mock_cursor

    result = get_existing_ids(connection)

    assert result == {"1", "2"}


def test_insert_and_fetch_greenhouses():
    connection = get_test_db()
    clean_tables(connection)
    create_tables(connection)

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
    clean_tables(connection)
    create_tables(connection)

    insert_into_cache(connection, "addr", 1.0, 2.0)

    result = get_from_cache(connection, "addr")

    assert result == (1.0, 2.0)

    connection.close()


def test_increment_attempt():
    connection = get_test_db()
    clean_tables(connection)
    create_tables(connection)

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
    clean_tables(connection)
    create_tables(connection)

    cursor = connection.cursor()
    cursor.execute(
        "INSERT INTO greenhouses_missing_location (id, attempts) VALUES (%s, %s)",
        ("1", 0),
    )
    connection.commit()

    result = fetch_missing_batch(connection, limit=10)

    assert len(result) == 1

    connection.close()


def test_get_last_sync_time_exists():

    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("2024-01-01T00:00:00",)

    connection = MagicMock()
    connection.cursor.return_value = mock_cursor

    result = get_last_sync_time(connection)

    assert result == "2024-01-01T00:00:00"


def test_get_last_sync_time_none():

    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None

    connection = MagicMock()
    connection.cursor.return_value = mock_cursor

    result = get_last_sync_time(connection)

    assert result is None


def test_get_from_cache_hit():

    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (10.0, 20.0)

    connection = MagicMock()
    connection.cursor.return_value = mock_cursor

    result = get_from_cache(connection, "addr")

    assert result == (10.0, 20.0)


def test_get_from_cache_miss():

    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None

    connection = MagicMock()
    connection.cursor.return_value = mock_cursor

    result = get_from_cache(connection, "addr")

    assert result is None


def test_delete_greenhouse():

    connection = MagicMock()
    connection.cursor.return_value = MagicMock()

    delete_greenhouse(connection, "1")

    assert connection.commit.called


# ------------------ WEATHER REPO ------------------


def test_clean_name():

    assert clean_name("Bangalore-East") == "Bangalore"
    assert clean_name("Delhi") == "Delhi"
    assert clean_name(None) == ""


def test_is_cache_fresh():

    fresh_time = datetime.now(UTC)
    old_time = datetime.now(UTC) - timedelta(hours=10)

    assert is_cache_fresh(fresh_time) is True
    assert is_cache_fresh(old_time) is False


def test_get_cached_weather():

    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (25, 0, 60, 10, "time")

    connection = MagicMock()
    connection.cursor.return_value = mock_cursor

    result = get_cached_weather(connection, "A")

    assert result["temperature"] == 25
    assert result["humidity"] == 60


def test_fetch_clusters():

    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("Bangalore-East", "North-1", 10.0, 20.0)]

    connection = MagicMock()
    connection.cursor.return_value = mock_cursor

    result = fetch_clusters(connection)

    assert result[0]["cluster_key"] == "Bangalore_North"


def test_upsert_weather_cache():

    connection = MagicMock()
    connection.cursor.return_value = MagicMock()

    cluster = {
        "cluster_key": "A",
        "latitude": 1,
        "longitude": 2,
        "temperature": 30,
        "rainfall": 0,
        "humidity": 50,
        "wind_speed": 10,
    }

    upsert_weather_cache(connection, cluster)

    assert connection.commit.called


def test_insert_weather_history():

    connection = MagicMock()
    connection.cursor.return_value = MagicMock()

    cluster = {
        "cluster_key": "A",
        "latitude": 1,
        "longitude": 2,
        "temperature": 30,
        "rainfall": 0,
        "humidity": 50,
        "wind_speed": 10,
    }

    insert_weather_history(connection, cluster)

    assert connection.commit.called
