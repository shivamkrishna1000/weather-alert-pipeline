import os
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

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
    fetch_clusters,
    get_cached_weather,
    insert_weather_history,
    is_cache_fresh,
    upsert_weather_cache,
)
from app.services.cluster_service import clean_name

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

    cursor = connection.cursor.return_value
    assert cursor.execute.called
    assert connection.commit.called


# ------------------ WEATHER REPO ------------------


def test_clean_name():

    assert clean_name("Bangalore-East") == "Bangalore"
    assert clean_name("Delhi") == "Delhi"
    assert clean_name(None) is None


def test_is_cache_fresh():
    now = datetime.now(UTC)

    assert is_cache_fresh(now) is True


def test_get_cached_weather():

    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (30, 20, 5, 80, 3, 60, 15, "time")

    connection = MagicMock()
    connection.cursor.return_value = mock_cursor

    result = get_cached_weather(connection, "A")

    assert result["max_temp"] == 30
    assert result["rain_hours"] == 3


def test_fetch_clusters():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        ("Bangalore-East", "North-1", "Village-1", 10.0, 20.0)
    ]
    mock_cursor.description = [
        ("district",),
        ("taluk",),
        ("village",),
        ("latitude",),
        ("longitude",),
    ]

    connection = MagicMock()
    connection.cursor.return_value = mock_cursor

    with patch(
        "app.repositories.weather_repo.get_cluster_mode",
        return_value="distance",
    ), patch(
        "app.repositories.weather_repo.build_distance_clusters",
        return_value=[
            {
                "cluster_key": "A",
                "latitude": 10.0,
                "longitude": 20.0,
                "members": [{"id": "1"}],
            }
        ],
    ):
        result = fetch_clusters(connection)

    assert result == [
        {
            "cluster_key": "A",
            "latitude": 10.0,
            "longitude": 20.0,
        }
    ]


def test_upsert_weather_cache():

    connection = MagicMock()
    connection.cursor.return_value = MagicMock()

    cluster = {
        "cluster_key": "A",
        "latitude": 1,
        "longitude": 2,
        "max_temp": 30,
        "min_temp": 20,
        "max_rain": 5,
        "rain_probability": 80,
        "rain_hours": 3,
        "max_humidity": 60,
        "max_wind": 15,
    }

    upsert_weather_cache(connection, cluster)

    cursor = connection.cursor.return_value
    assert cursor.execute.called
    assert connection.commit.called


def test_insert_weather_history():

    connection = MagicMock()
    connection.cursor.return_value = MagicMock()

    cluster = {
        "cluster_key": "A",
        "latitude": 1,
        "longitude": 2,
        "max_temp": 30,
        "min_temp": 20,
        "max_rain": 5,
        "rain_probability": 80,
        "rain_hours": 3,
        "max_humidity": 60,
        "max_wind": 15,
    }

    insert_weather_history(connection, cluster)

    cursor = connection.cursor.return_value
    assert cursor.execute.called
    assert connection.commit.called


def test_get_cached_weather_none():
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None

    connection = MagicMock()
    connection.cursor.return_value = mock_cursor

    result = get_cached_weather(connection, "A")

    assert result is None


# ------------------ ADVISORY REPO ------------------


def test_fetch_greenhouses_by_cluster():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        ("1", "Ravi", "999"),
        ("2", "Amit", "888"),
    ]
    mock_cursor.description = [
        ("id",),
        ("farmer_name",),
        ("phone",),
    ]

    connection = MagicMock()
    connection.cursor.return_value = mock_cursor

    from app.repositories.advisory_repo import fetch_greenhouses_by_cluster

    result = fetch_greenhouses_by_cluster(connection, "A")

    assert result == [
        {"id": "1", "farmer_name": "Ravi", "phone": "999"},
        {"id": "2", "farmer_name": "Amit", "phone": "888"},
    ]

    mock_cursor.execute.assert_called_once()
    mock_cursor.close.assert_called_once()


def test_advisory_already_sent_true():
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (1,)

    connection = MagicMock()
    connection.cursor.return_value = mock_cursor

    from app.repositories.advisory_repo import advisory_already_sent

    result = advisory_already_sent(connection, "1", "Rain alert")

    assert result is True
    mock_cursor.close.assert_called_once()


def test_advisory_already_sent_false():
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None

    connection = MagicMock()
    connection.cursor.return_value = mock_cursor

    from app.repositories.advisory_repo import advisory_already_sent

    result = advisory_already_sent(connection, "1", "Rain alert")

    assert result is False


def test_insert_advisory_log():
    mock_cursor = MagicMock()

    connection = MagicMock()
    connection.cursor.return_value = mock_cursor

    greenhouse = {
        "id": "1",
        "name": "GH-3",
        "farmer_name": "Ravi",
        "phone": "999",
    }

    from app.repositories.advisory_repo import insert_advisory_log

    insert_advisory_log(connection, greenhouse, "A", "Rain alert")

    assert mock_cursor.execute.called
    assert connection.commit.called
    mock_cursor.close.assert_called_once()
