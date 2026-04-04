from unittest.mock import MagicMock, patch

import pytest

from app.external.maps_client import geocode_address
from app.external.zoho_client import extract_records, fetch_all_greenhouse_data

# ------------------ MAPS CLIENT ------------------


@patch("app.external.maps_client.get_google_maps_api_key", return_value="fake-key")
@patch("app.external.maps_client.requests.get")
def test_geocode_address_success(mock_get, mock_key):
    mock_get.return_value.status_code = 200
    mock_get.return_value.raise_for_status = MagicMock()
    mock_get.return_value.json.return_value = {
        "status": "OK",
        "results": [{"geometry": {"location": {"lat": 10.0, "lng": 20.0}}}],
    }

    lat, lon = geocode_address("test")

    assert lat == 10.0
    assert lon == 20.0


@patch("app.external.maps_client.requests.get")
def test_geocode_address_failure(mock_get):
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"status": "ZERO_RESULTS"}

    with pytest.raises(ValueError):
        geocode_address("bad")


@patch("app.external.maps_client.get_google_maps_api_key", return_value="fake-key")
@patch("app.external.maps_client.requests.get")
def test_geocode_address_request_exception(mock_get, mock_key):
    import pytest
    import requests

    from app.external.maps_client import geocode_address

    mock_get.side_effect = requests.exceptions.RequestException("network error")

    with pytest.raises(RuntimeError):
        geocode_address("test")


# ------------------ ZOHO CLIENT ------------------


class DummyResponse:
    def __init__(self, data):
        self._data = data
        self.text = "x"

    def json(self):
        return self._data


def test_get_valid_access_token_cached(monkeypatch):
    from app.external import zoho_client

    zoho_client._access_token = "cached"
    zoho_client._expiry_time = 9999999999  # future

    token = zoho_client.get_valid_access_token()

    assert token == "cached"


@patch("app.external.zoho_client.get_zoho_client_id", return_value="id")
@patch("app.external.zoho_client.get_zoho_client_secret", return_value="secret")
@patch("app.external.zoho_client.get_zoho_refresh_token", return_value="refresh")
@patch("app.external.zoho_client.requests.post")
def test_refresh_access_token(mock_post, mock_refresh, mock_secret, mock_id):
    from app.external.zoho_client import refresh_access_token

    mock_post.return_value.json.return_value = {
        "access_token": "new_token",
        "expires_in": 3600,
    }
    mock_post.return_value.raise_for_status = lambda: None

    token = refresh_access_token()

    assert token == "new_token"


def test_extract_records_success():
    response = DummyResponse({"data": [1, 2]})

    result = extract_records(response)

    assert result == [1, 2]


def test_extract_records_empty():
    response = DummyResponse({})

    result = extract_records(response)

    assert result == []


@patch("app.external.zoho_client.fetch_page")
@patch("app.external.zoho_client.get_valid_access_token", return_value="token")
@patch("app.external.zoho_client.get_last_sync_time", return_value=None)
def test_fetch_all_greenhouse_data_multiple_pages(mock_sync, mock_token, mock_fetch):

    response1 = MagicMock()
    response1.text = "x"
    response1.json.return_value = {"data": [{"id": "1"}]}

    response2 = MagicMock()
    response2.text = "x"
    response2.json.return_value = {"data": []}

    mock_fetch.side_effect = [response1, response2]

    result = fetch_all_greenhouse_data(connection=None)

    assert len(result) == 1


@patch("app.external.zoho_client.requests.get")
@patch("app.external.zoho_client.get_valid_access_token", return_value="fake-token")
@patch("app.external.zoho_client.get_last_sync_time", return_value=None)
def test_fetch_all_greenhouse_data_empty_response(mock_sync, mock_token, mock_get):
    response = MagicMock()
    response.text = ""

    mock_get.return_value = response

    result = fetch_all_greenhouse_data(connection=None)

    assert result == []
