from unittest.mock import MagicMock, call, patch

import pytest

from app.main import main


@patch("app.main.run_weather_pipeline")
@patch("app.main.run_geocode_pipeline")
@patch("app.main.run_sync_pipeline")
@patch("app.main.get_connection")
@patch("app.main.load_environment")
@patch("app.main.get_database_url")
def test_main_runs_successfully(
    mock_get_db_url,
    mock_load_env,
    mock_conn,
    mock_sync,
    mock_geocode,
    mock_weather,
):

    mock_get_db_url.return_value = "fake_db_url"

    mock_connection = MagicMock()
    mock_conn.return_value = mock_connection

    main()

    mock_load_env.assert_called_once()
    mock_conn.assert_called_once_with("fake_db_url")
    mock_sync.assert_called_once_with(mock_connection)
    mock_geocode.assert_called_once_with(mock_connection, batch_size=100)
    mock_weather.assert_called_once_with(mock_connection)

    # 🔴 missing earlier
    mock_connection.close.assert_called_once()


@patch("app.main.load_environment")
@patch("app.main.get_database_url", side_effect=ValueError("Missing DB URL"))
def test_main_handles_db_error(mock_get_db_url, mock_load_env):

    with pytest.raises(ValueError):
        main()


@patch("app.main.run_weather_pipeline")
@patch("app.main.run_geocode_pipeline")
@patch("app.main.run_sync_pipeline")
@patch("app.main.get_connection")
@patch("app.main.load_environment")
@patch("app.main.get_database_url")
def test_main_flow_calls(
    mock_get_db_url,
    mock_load_env,
    mock_conn,
    mock_sync,
    mock_geocode,
    mock_weather,
):

    mock_get_db_url.return_value = "fake_db_url"
    mock_connection = MagicMock()
    mock_conn.return_value = mock_connection

    main()

    assert mock_sync.called
    assert mock_geocode.called
    assert mock_weather.called


@patch("app.main.run_weather_pipeline")
@patch("app.main.run_geocode_pipeline")
@patch("app.main.run_sync_pipeline")
@patch("app.main.get_connection")
@patch("app.main.load_environment")
@patch("app.main.get_database_url")
def test_main_execution_order(
    mock_get_db_url,
    mock_load_env,
    mock_conn,
    mock_sync,
    mock_geocode,
    mock_weather,
):

    mock_get_db_url.return_value = "fake_db_url"
    mock_connection = mock_conn.return_value

    main()

    expected_calls = [
        call(mock_connection),
        call(mock_connection, batch_size=100),
        call(mock_connection),
    ]

    actual_calls = [
        mock_sync.call_args,
        mock_geocode.call_args,
        mock_weather.call_args,
    ]

    assert actual_calls == expected_calls
