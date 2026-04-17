from unittest.mock import MagicMock, patch

import pytest

from app.constants import ALLOWED_STATUSES
from app.pipelines.geocode_pipeline import (
    get_coordinates,
    persist_geocoded_result,
    process_record,
    run_geocode_pipeline,
)
from app.pipelines.sync_pipeline import run_sync_pipeline
from app.pipelines.weather_pipeline import run_weather_pipeline

# ------------------ GEOCODE PIPELINE ------------------


def test_process_record_success():
    record = {"id": "1", "attempts": 0}

    with patch(
        "app.pipelines.geocode_pipeline.prepare_address", return_value="addr"
    ), patch("app.pipelines.geocode_pipeline.should_retry", return_value=True), patch(
        "app.pipelines.geocode_pipeline.get_coordinates", return_value=(1.0, 2.0)
    ), patch(
        "app.pipelines.geocode_pipeline.persist_geocoded_result"
    ) as mock_persist:

        result = process_record(None, record)

        assert result is True
        mock_persist.assert_called_once()


def test_process_record_skip_empty_address():
    record = {"id": "1", "attempts": 0}

    with patch("app.pipelines.geocode_pipeline.prepare_address", return_value=None):
        result = process_record(None, record)

        assert result is False


def test_process_record_max_attempts():
    record = {"id": "1", "attempts": 5}

    with patch(
        "app.pipelines.geocode_pipeline.prepare_address", return_value="addr"
    ), patch("app.pipelines.geocode_pipeline.should_retry", return_value=False):

        result = process_record(None, record)

        assert result is False


def test_process_record_no_address():

    with patch("app.pipelines.geocode_pipeline.prepare_address", return_value=None):
        result = process_record(None, {"id": "1"})

        assert result is False


def test_process_record_exception():

    with patch(
        "app.pipelines.geocode_pipeline.prepare_address", return_value="addr"
    ), patch("app.pipelines.geocode_pipeline.should_retry", return_value=True), patch(
        "app.pipelines.geocode_pipeline.get_coordinates",
        side_effect=RuntimeError("fail"),
    ):

        result = process_record(None, {"id": "1", "attempts": 0})

        assert result is False


def test_get_coordinates_cache_hit():

    with patch(
        "app.pipelines.geocode_pipeline.get_from_cache", return_value=(1.0, 2.0)
    ):
        result = get_coordinates(None, "addr", "1")

        assert result == (1.0, 2.0)


def test_get_coordinates_api_success():
    with patch(
        "app.pipelines.geocode_pipeline.get_from_cache", return_value=None
    ), patch(
        "app.pipelines.geocode_pipeline.geocode_address", return_value=(1.0, 2.0)
    ), patch(
        "app.pipelines.geocode_pipeline.insert_into_cache"
    ) as mock_insert:

        result = get_coordinates(None, "addr", "1")

        assert result == (1.0, 2.0)
        mock_insert.assert_called_once()


def test_get_coordinates_value_error():
    with patch(
        "app.pipelines.geocode_pipeline.get_from_cache", return_value=None
    ), patch(
        "app.pipelines.geocode_pipeline.geocode_address",
        side_effect=ValueError("bad"),
    ), patch(
        "app.pipelines.geocode_pipeline.handle_failed_geocode"
    ) as mock_handle:

        with pytest.raises(ValueError):
            get_coordinates(None, "addr", "1")

        mock_handle.assert_called_once_with(None, "1")


def test_get_coordinates_runtime_error():
    with patch(
        "app.pipelines.geocode_pipeline.get_from_cache", return_value=None
    ), patch(
        "app.pipelines.geocode_pipeline.geocode_address",
        side_effect=RuntimeError("fail"),
    ), patch(
        "app.pipelines.geocode_pipeline.handle_failed_geocode"
    ):

        with pytest.raises(RuntimeError):
            get_coordinates(None, "addr", "1")


def test_persist_geocoded_result():
    record = {"id": "1"}

    with patch(
        "app.pipelines.geocode_pipeline.insert_geocoded_record"
    ) as mock_insert, patch(
        "app.pipelines.geocode_pipeline.delete_from_missing"
    ) as mock_delete:

        persist_geocoded_result(None, record, 1.0, 2.0)

        mock_insert.assert_called_once()
        mock_delete.assert_called_once()


def test_run_geocode_pipeline_single_record():
    record = {"id": "1"}

    with patch(
        "app.pipelines.geocode_pipeline.fetch_missing_batch",
        side_effect=[[record], []],
    ), patch(
        "app.pipelines.geocode_pipeline.process_record_parallel",
        return_value=True,
    ), patch(
        "app.pipelines.geocode_pipeline.get_connection",
        return_value=MagicMock(),
    ):

        run_geocode_pipeline(None, database_url="dummy")


def test_get_coordinates_value_error_triggers_retry():
    with patch(
        "app.pipelines.geocode_pipeline.get_from_cache", return_value=None
    ), patch(
        "app.pipelines.geocode_pipeline.geocode_address",
        side_effect=ValueError("bad"),
    ), patch(
        "app.pipelines.geocode_pipeline.increment_attempt"
    ) as mock_increment:

        with pytest.raises(ValueError):
            get_coordinates(None, "addr", "1")

        mock_increment.assert_called_once_with(None, "1")


# ------------------ SYNC PIPELINE ------------------


@patch("app.pipelines.sync_pipeline.delete_greenhouse")
@patch("app.pipelines.sync_pipeline.create_tables")
@patch("app.pipelines.sync_pipeline.fetch_all_greenhouse_data")
@patch("app.pipelines.sync_pipeline.process_greenhouse_records")
@patch("app.pipelines.sync_pipeline.insert_greenhouses")
@patch("app.pipelines.sync_pipeline.insert_missing_location")
@patch("app.pipelines.sync_pipeline.update_last_sync_time")
def test_run_sync_pipeline_success(
    mock_update,
    mock_insert_missing,
    mock_insert,
    mock_process,
    mock_fetch,
    mock_create,
    mock_delete,
):
    mock_fetch.return_value = [
        {
            "Modified_Time": "2024-01-01T00:00:00",
            "id": "1",
            "Current_GH_Status": list(ALLOWED_STATUSES)[0],
        }
    ]
    mock_process.return_value = ([], [])

    run_sync_pipeline(None)

    mock_create.assert_called_once()
    mock_fetch.assert_called_once()
    mock_insert.assert_called_once()
    mock_insert_missing.assert_called_once()
    mock_update.assert_called_once()


@patch("app.pipelines.sync_pipeline.fetch_all_greenhouse_data", return_value=[])
@patch("app.pipelines.sync_pipeline.create_tables")
def test_sync_pipeline_no_records(mock_create, mock_fetch):

    run_sync_pipeline(None)

    mock_create.assert_called_once()


@patch("app.pipelines.sync_pipeline.insert_greenhouses")
@patch("app.pipelines.sync_pipeline.insert_missing_location")
@patch("app.pipelines.sync_pipeline.process_greenhouse_records", return_value=([], []))
@patch("app.pipelines.sync_pipeline.delete_greenhouse")
@patch("app.pipelines.sync_pipeline.fetch_all_greenhouse_data")
@patch("app.pipelines.sync_pipeline.create_tables")
def test_sync_pipeline_invalid_records_deleted(
    mock_create,
    mock_fetch,
    mock_delete,
    mock_process,
    mock_insert_missing,
    mock_insert,
):

    mock_fetch.return_value = [{"id": "1", "Current_GH_Status": "invalid"}]

    run_sync_pipeline(None)

    mock_delete.assert_called_once_with(None, "1")


# ------------------ WEATHER PIPELINE ------------------


def test_weather_pipeline_cache_hit():
    connection = object()

    clusters = [{"cluster_key": "A", "latitude": 1, "longitude": 2}]

    with patch(
        "app.pipelines.weather_pipeline.fetch_clusters", return_value=clusters
    ), patch(
        "app.pipelines.weather_pipeline.get_cached_weather",
        return_value={"fetched_at": "now"},
    ), patch(
        "app.pipelines.weather_pipeline.is_cache_fresh", return_value=True
    ), patch(
        "app.pipelines.weather_pipeline.get_weather"
    ) as mock_weather:

        run_weather_pipeline(connection)

        mock_weather.assert_not_called()


def test_weather_pipeline_fetch_and_store():
    connection = object()

    clusters = [{"cluster_key": "A", "latitude": 1, "longitude": 2}]

    weather_data = {
        "max_temp": 30,
        "min_temp": 20,
        "max_rain": 0,
        "rain_probability": 0,
        "rain_hours": 0,
        "max_humidity": 50,
        "max_wind": 10,
    }

    with patch(
        "app.pipelines.weather_pipeline.fetch_clusters", return_value=clusters
    ), patch(
        "app.pipelines.weather_pipeline.get_cached_weather", return_value=None
    ), patch(
        "app.pipelines.weather_pipeline.get_weather", return_value=weather_data
    ), patch(
        "app.pipelines.weather_pipeline.upsert_weather_cache"
    ) as mock_cache, patch(
        "app.pipelines.weather_pipeline.insert_weather_history"
    ) as mock_history:

        run_weather_pipeline(connection)

        mock_cache.assert_called_once()
        mock_history.assert_called_once()


def test_weather_pipeline_api_failure():
    connection = MagicMock()

    clusters = [{"cluster_key": "A", "latitude": 1, "longitude": 2}]

    with patch(
        "app.pipelines.weather_pipeline.fetch_clusters", return_value=clusters
    ), patch(
        "app.pipelines.weather_pipeline.get_cached_weather", return_value=None
    ), patch(
        "app.pipelines.weather_pipeline.get_weather",
        side_effect=RuntimeError("fail"),
    ), patch(
        "app.pipelines.weather_pipeline.upsert_weather_cache"
    ) as mock_cache:

        run_weather_pipeline(connection)

        mock_cache.assert_not_called()
