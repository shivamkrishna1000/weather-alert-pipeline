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


def test_normalize_weather():

    data = {
        "current": {
            "temp_c": 30,
            "humidity": 50,
            "precip_mm": 1,
            "wind_kph": 10,
        }
    }

    result = normalize_weather(data)

    assert result["temperature"] == 30
    assert result["humidity"] == 50
    assert result["rainfall"] == 1
    assert result["wind_speed"] == 10


def test_normalize_weather_missing_fields():

    data = {"current": {}}

    result = normalize_weather(data)

    assert result["temperature"] is None
    assert result["humidity"] is None


def test_get_weather_calls_fetch():

    raw_data = {
        "current": {
            "temp_c": 25,
            "humidity": 60,
            "precip_mm": 0,
            "wind_kph": 5,
        }
    }

    with patch(
        "app.services.weather_service.fetch_weather_raw", return_value=raw_data
    ) as mock_fetch:

        result = get_weather(1, 2)

        mock_fetch.assert_called_once_with(1, 2)
        assert result["temperature"] == 25
