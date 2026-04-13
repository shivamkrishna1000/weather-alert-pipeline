from unittest.mock import patch

from app.constants import ZOHO_FIELDS
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
                    * 12
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
                    "hour": [
                        {
                            "time": "2026-04-13 10:00",
                            "temp_c": 25,
                            "humidity": 60,
                            "precip_mm": 0,
                            "chance_of_rain": 0,
                            "will_it_rain": 0,
                            "wind_kph": 5,
                        }
                    ]
                    * 12
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
