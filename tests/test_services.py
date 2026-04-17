from unittest.mock import patch

import pytest

from app.constants import ZOHO_FIELDS
from app.services.cluster_service import build_cluster_key, build_distance_clusters
from app.services.geocode_service import should_retry
from app.services.greenhouse_service import process_greenhouse_records
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
