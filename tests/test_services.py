from unittest.mock import MagicMock, patch

import pytest
import requests

from app.constants import ZOHO_FIELDS
from app.services.advisory_service import generate_advisories
from app.services.cluster_service import build_cluster_key, build_distance_clusters
from app.services.delivery_service import (
    format_greenhouse_message,
    group_advisories_by_farmer,
)
from app.services.geocode_service import should_retry
from app.services.greenhouse_service import process_greenhouse_records
from app.services.wati_service import send_whatsapp_message
from app.services.weather_service import get_weather, normalize_weather

# ------------------ GREENHOUSE SERVICE ------------------


def test_process_greenhouse_records_split():
    records = [
        {
            ZOHO_FIELDS["status"]: "2. FS taken over and being used",
            ZOHO_FIELDS["latitude"]: 17.1,
            ZOHO_FIELDS["longitude"]: 78.1,
            ZOHO_FIELDS["id"]: "1",
        },
        {
            ZOHO_FIELDS["status"]: "2. FS taken over and being used",
            ZOHO_FIELDS["latitude"]: None,
            ZOHO_FIELDS["longitude"]: None,
            ZOHO_FIELDS["id"]: "2",
        },
    ]

    with_loc, without_loc = process_greenhouse_records(records)

    assert len(with_loc) == 1
    assert len(without_loc) == 1


# ------------------ GEOCODE SERVICE ------------------


def test_should_retry_logic():
    assert should_retry(0) is True
    assert should_retry(2) is True
    assert should_retry(3) is False


# ------------------ WEATHER SERVICE ------------------


def test_normalize_weather_forecast_structure():

    data = {
        "location": {"localtime": "2026-04-13 10:00"},
        "forecast": {
            "forecastday": [
                {
                    "day": {
                        "maxtemp_c": 30,
                        "mintemp_c": 20,
                        "daily_chance_of_rain": 60,
                        "maxwind_kph": 10,
                    },
                    "hour": [
                        {
                            "time": "2026-04-13 10:00",
                            "temp_c": 30,
                            "humidity": 50,
                            "precip_mm": 1,
                            "chance_of_rain": 60,
                            "will_it_rain": 1,
                            "wind_kph": 10,
                        }
                    ]
                    * 12,
                }
            ]
        },
    }

    result = normalize_weather(data)

    assert result["max_temp"] == 30
    assert result["rain_hours"] == 12


def test_get_weather_calls_fetch():

    raw_data = {
        "location": {"localtime": "2026-04-13 10:00"},
        "forecast": {
            "forecastday": [
                {
                    "day": {
                        "maxtemp_c": 30,
                        "mintemp_c": 20,
                        "daily_chance_of_rain": 60,
                        "maxwind_kph": 10,
                    },
                    "hour": [
                        {
                            "time": "2026-04-13 10:00",
                            "temp_c": 30,
                            "humidity": 50,
                            "precip_mm": 1,
                            "chance_of_rain": 60,
                            "will_it_rain": 1,
                            "wind_kph": 10,
                        }
                    ]
                    * 12,
                }
            ]
        },
    }

    with patch(
        "app.services.weather_service.fetch_weather_raw", return_value=raw_data
    ) as mock_fetch:

        result = get_weather(1, 2)

        mock_fetch.assert_called_once_with(1, 2)
        assert "max_temp" in result


def test_normalize_weather_no_hours():
    data = {
        "forecast": {
            "forecastday": [
                {
                    "day": {
                        "maxtemp_c": 30,
                        "mintemp_c": 20,
                        "daily_chance_of_rain": 0,
                        "maxwind_kph": 5,
                    },
                    "hour": [],
                }
            ]
        }
    }

    with pytest.raises(RuntimeError):
        normalize_weather(data)


# ------------------ CLUSTER SERVICE ------------------


def test_build_distance_clusters_basic():
    records = [
        {"latitude": 10.0, "longitude": 20.0},
        {"latitude": 10.001, "longitude": 20.001},
    ]

    result = build_distance_clusters(records)

    assert len(result) == 1
    assert "cluster_key" in result[0]


def test_build_cluster_key_taluk_mode():
    record = {
        "district": "Bangalore-East",
        "taluk": "North-1",
        "village": "X",
    }

    with patch("app.services.cluster_service.get_cluster_mode", return_value="taluk"):
        result = build_cluster_key(record)

    assert result == "taluk_Bangalore_North"


# ------------------ WATI SERVICE ------------------


@patch("app.services.wati_service.is_debug_mode", return_value=True)
def test_send_whatsapp_debug_mode(mock_debug):
    result = send_whatsapp_message("919999999999", "Ravi", "Test message")

    assert result is False


@patch("app.services.wati_service.is_debug_mode", return_value=False)
@patch("app.services.wati_service.get_wati_template_name", return_value="template")
@patch("app.services.wati_service.get_wati_api_token", return_value="token")
@patch("app.services.wati_service.get_wati_base_url", return_value="http://test")
@patch("app.services.wati_service.requests.post")
def test_send_whatsapp_success(
    mock_post, mock_url, mock_token, mock_template, mock_debug
):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"result": True}
    mock_response.raise_for_status.return_value = None

    mock_post.return_value = mock_response

    result = send_whatsapp_message("919999999999", "Ravi", "Hello")

    assert result is True


@patch("app.services.wati_service.is_debug_mode", return_value=False)
@patch("app.services.wati_service.get_wati_template_name", return_value="template")
@patch("app.services.wati_service.get_wati_api_token", return_value="token")
@patch("app.services.wati_service.get_wati_base_url", return_value="http://test")
@patch(
    "app.services.wati_service.requests.post",
    side_effect=requests.exceptions.RequestException("fail"),
)
def test_send_whatsapp_api_failure(
    mock_post, mock_url, mock_token, mock_template, mock_debug
):
    result = send_whatsapp_message("919999999999", "Ravi", "Hello")

    assert result is False


@patch("app.services.wati_service.is_debug_mode", return_value=False)
@patch("app.services.wati_service.get_wati_template_name", return_value="template")
@patch("app.services.wati_service.get_wati_api_token", return_value="token")
@patch("app.services.wati_service.get_wati_base_url", return_value="http://test")
@patch("app.services.wati_service.requests.post")
def test_send_whatsapp_invalid_json(
    mock_post, mock_url, mock_token, mock_template, mock_debug
):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.side_effect = ValueError("bad json")
    mock_response.raise_for_status.return_value = None

    mock_post.return_value = mock_response

    result = send_whatsapp_message("919999999999", "Ravi", "Hello")

    assert result is False


@patch("app.services.wati_service.is_debug_mode", return_value=False)
@patch("app.services.wati_service.get_wati_template_name", return_value="template")
@patch("app.services.wati_service.get_wati_api_token", return_value="token")
@patch("app.services.wati_service.get_wati_base_url", return_value="http://test")
@patch("app.services.wati_service.requests.post")
def test_send_whatsapp_result_false(
    mock_post, mock_url, mock_token, mock_template, mock_debug
):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"result": False}
    mock_response.raise_for_status.return_value = None

    mock_post.return_value = mock_response

    result = send_whatsapp_message("919999999999", "Ravi", "Hello")

    assert result is False


# ------------------ DELIVERY SERVICE ------------------


def test_group_advisories():
    records = [
        {
            "phone": "999",
            "farmer_name": "Ravi",
            "greenhouse_name": "GH1",
            "advisory": "Rain alert",
            "id": 1,
        },
        {
            "phone": "999",
            "farmer_name": "Ravi",
            "greenhouse_name": "GH1",
            "advisory": "Wind alert",
            "id": 2,
        },
    ]

    result = group_advisories_by_farmer(records)

    assert "999" in result
    assert len(result["999"]["greenhouses"]["GH1"]) == 2


def test_group_advisories_skip_invalid():
    records = [
        {
            "phone": None,
            "farmer_name": "Ravi",
            "greenhouse_name": "GH1",
            "advisory": "Rain alert",
            "id": 1,
        }
    ]

    result = group_advisories_by_farmer(records)

    assert result == {}


def test_format_message():
    result = format_greenhouse_message("GH1", ["Rain", "Wind"])

    assert "GH1" in result
    assert "Rain" in result
    assert "Wind" in result


# ------------------ ADVISORY SERVICE ------------------


def test_rain_rule_triggers():
    weather = {
        "max_temp": 30,
        "min_temp": 20,
        "max_rain": 25,
        "rain_probability": 90,
        "rain_hours": 5,
        "max_humidity": 50,
        "max_wind": 5,
    }

    result = generate_advisories(weather)

    assert any("rain" in r.lower() for r in result)


def test_wind_rule_triggers():
    weather = {
        "max_temp": 30,
        "min_temp": 20,
        "max_rain": 0,
        "rain_probability": 0,
        "rain_hours": 0,
        "max_humidity": 50,
        "max_wind": 30,
    }

    result = generate_advisories(weather)

    assert any("wind" in r.lower() for r in result)


def test_humidity_rule_triggers():
    weather = {
        "max_temp": 30,
        "min_temp": 20,
        "max_rain": 0,
        "rain_probability": 0,
        "rain_hours": 0,
        "max_humidity": 95,
        "max_wind": 5,
    }

    result = generate_advisories(weather)

    assert any("humidity" in r.lower() for r in result)


def test_combined_conditions():
    weather = {
        "max_temp": 35,
        "min_temp": 25,
        "max_rain": 20,
        "rain_probability": 80,
        "rain_hours": 4,
        "max_humidity": 90,
        "max_wind": 25,
    }

    result = generate_advisories(weather)

    # Should include multiple categories
    assert len(result) >= 2


def test_rain_overrides_temperature():
    weather = {
        "max_temp": 40,  # would trigger temp rule
        "min_temp": 20,
        "max_rain": 30,  # rain present
        "rain_probability": 90,
        "rain_hours": 5,
        "max_humidity": 50,
        "max_wind": 5,
    }

    result = generate_advisories(weather)

    # temperature irrigation advice should be suppressed
    assert not any("temperature" in r.lower() for r in result)


def test_no_rules_triggered():
    weather = {
        "max_temp": 25,
        "min_temp": 20,
        "max_rain": 0,
        "rain_probability": 0,
        "rain_hours": 0,
        "max_humidity": 40,
        "max_wind": 5,
    }

    result = generate_advisories(weather)

    assert result == []


def test_boundary_conditions():
    weather = {
        "max_temp": 35,  # boundary
        "min_temp": 15,
        "max_rain": 0,
        "rain_probability": 50,
        "rain_hours": 0,
        "max_humidity": 80,
        "max_wind": 10,
    }

    result = generate_advisories(weather)

    assert isinstance(result, list)
