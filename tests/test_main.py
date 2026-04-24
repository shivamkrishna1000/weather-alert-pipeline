from unittest.mock import MagicMock, patch

import pytest

from app.main import main

# ------------------ NO ARGUMENT ------------------


def test_main_no_args(capsys):
    with patch("app.main.load_environment"):
        with patch("sys.argv", ["main.py"]):
            main()

    captured = capsys.readouterr()
    assert "Usage" in captured.out


# ------------------ INVALID MODE ------------------


def test_main_invalid_mode(capsys):
    with patch("app.main.load_environment"), patch(
        "app.main.get_database_url", return_value="db"
    ), patch("app.main.get_connection", return_value=MagicMock()), patch(
        "sys.argv", ["main.py", "invalid"]
    ):

        main()

    captured = capsys.readouterr()
    assert "Invalid mode" in captured.out


# ------------------ WEEKLY ------------------


@patch("app.main.run_weekly_pipeline")
@patch("app.main.get_connection")
@patch("app.main.get_database_url", return_value="db")
@patch("app.main.load_environment")
def test_main_weekly(mock_env, mock_db, mock_conn, mock_weekly):
    connection = MagicMock()
    mock_conn.return_value = connection

    with patch("sys.argv", ["main.py", "weekly"]):
        main()

    mock_weekly.assert_called_once_with(connection, "db")
    connection.close.assert_called_once()


# ------------------ DAILY ------------------


@patch("app.main.run_daily_pipeline")
@patch("app.main.get_connection")
@patch("app.main.get_database_url", return_value="db")
@patch("app.main.load_environment")
def test_main_daily(mock_env, mock_db, mock_conn, mock_daily):
    connection = MagicMock()
    mock_conn.return_value = connection

    with patch("sys.argv", ["main.py", "daily"]):
        main()

    mock_daily.assert_called_once_with(connection)
    connection.close.assert_called_once()


# ------------------ DB URL FAILURE ------------------


@patch("app.main.get_database_url", side_effect=ValueError("fail"))
@patch("app.main.load_environment")
def test_main_db_url_failure(mock_env, mock_db):
    with patch("sys.argv", ["main.py", "weekly"]):
        with pytest.raises(ValueError):
            main()


# ------------------ CONNECTION FAILURE ------------------


@patch("app.main.get_connection", side_effect=RuntimeError("fail"))
@patch("app.main.get_database_url", return_value="db")
@patch("app.main.load_environment")
def test_main_connection_failure(mock_env, mock_db, mock_conn):
    with patch("sys.argv", ["main.py", "weekly"]):
        with pytest.raises(RuntimeError):
            main()


# ------------------ PIPELINE FAILURE ------------------


@patch("app.main.run_daily_pipeline", side_effect=RuntimeError("fail"))
@patch("app.main.get_connection")
@patch("app.main.get_database_url", return_value="db")
@patch("app.main.load_environment")
def test_main_pipeline_failure(mock_env, mock_db, mock_conn, mock_pipeline):
    connection = MagicMock()
    mock_conn.return_value = connection

    with patch("sys.argv", ["main.py", "daily"]):
        with pytest.raises(RuntimeError):
            main()

    # IMPORTANT: finally block
    connection.close.assert_called_once()
