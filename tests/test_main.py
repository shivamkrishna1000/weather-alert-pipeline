from unittest.mock import MagicMock, patch


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
):
    from app.main import main

    mock_get_db_url.return_value = "fake_db_url"

    mock_connection = MagicMock()
    mock_conn.return_value = mock_connection

    main()

    mock_load_env.assert_called_once()
    mock_conn.assert_called_once_with("fake_db_url")
    mock_sync.assert_called_once_with(mock_connection)
    mock_geocode.assert_called_once_with(mock_connection, batch_size=100)

    # 🔴 missing earlier
    mock_connection.close.assert_called_once()


@patch("app.main.load_environment")
@patch("app.main.get_database_url", return_value=None)
def test_main_missing_db_url(mock_get_db_url, mock_load_env):
    from app.main import main

    # main handles error internally → no exception expected
    main()


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
):
    from app.main import main

    mock_get_db_url.return_value = "fake_db_url"
    mock_connection = MagicMock()
    mock_conn.return_value = mock_connection

    main()

    assert mock_sync.called
    assert mock_geocode.called
