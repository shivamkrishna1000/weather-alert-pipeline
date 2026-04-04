from unittest.mock import patch

from app.constants import ALLOWED_STATUSES
from app.pipelines.geocode_pipeline import (
    get_coordinates,
    persist_geocoded_result,
    process_record,
    run_geocode_pipeline,
)
from app.pipelines.sync_pipeline import run_sync_pipeline


def test_process_record_success():
    record = {"id": "1", "attempts": 0}

    with patch(
        "app.pipelines.geocode_pipeline.prepare_address", return_value="addr"
    ), patch("app.pipelines.geocode_pipeline.should_retry", return_value=True), patch(
        "app.pipelines.geocode_pipeline.get_coordinates", return_value=(1.0, 2.0)
    ), patch(
        "app.pipelines.geocode_pipeline.persist_geocoded_result"
    ):

        result = process_record(None, record)

        assert result is True


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
    from app.pipelines.sync_pipeline import run_sync_pipeline

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
    from app.pipelines.sync_pipeline import run_sync_pipeline

    mock_fetch.return_value = [{"id": "1", "Current_GH_Status": "invalid"}]

    run_sync_pipeline(None)

    mock_delete.assert_called_once_with(None, "1")


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
        "app.pipelines.geocode_pipeline.geocode_address", side_effect=ValueError
    ), patch(
        "app.pipelines.geocode_pipeline.increment_attempt"
    ) as mock_inc:

        result = get_coordinates(None, "addr", "1")

        assert result is None
        mock_inc.assert_called_once()


def test_get_coordinates_runtime_error():
    with patch(
        "app.pipelines.geocode_pipeline.get_from_cache", return_value=None
    ), patch(
        "app.pipelines.geocode_pipeline.geocode_address",
        side_effect=RuntimeError("fail"),
    ):

        result = get_coordinates(None, "addr", "1")

        assert result is None


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
        "app.pipelines.geocode_pipeline.fetch_missing_batch", side_effect=[[record], []]
    ), patch("app.pipelines.geocode_pipeline.process_record", return_value=True), patch(
        "app.pipelines.geocode_pipeline.time.sleep"
    ):

        run_geocode_pipeline(None)
