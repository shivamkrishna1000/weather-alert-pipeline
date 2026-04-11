from app.external.weather_client import fetch_weather_raw


def normalize_weather(data: dict) -> dict:
    """
    Normalize raw WeatherAPI response.

    Returns
    -------
    dict
        {
            temperature,
            humidity,
            rainfall,
            wind_speed
        }
    """

    current = data["current"]

    return {
        "temperature": current.get("temp_c"),
        "humidity": current.get("humidity"),
        "rainfall": current.get("precip_mm"),
        "wind_speed": current.get("wind_kph"),
    }


def get_weather(latitude: float, longitude: float) -> dict:
    """
    Fetch and normalize weather data.

    Returns
    -------
    dict
        Normalized weather data
    """

    raw_data = fetch_weather_raw(latitude, longitude)

    return normalize_weather(raw_data)
