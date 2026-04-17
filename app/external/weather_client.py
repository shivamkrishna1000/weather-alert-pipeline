import requests

from app.config import get_weather_api_key


def fetch_weather_raw(latitude: float, longitude: float) -> dict:
    """
    Fetch raw weather forecast data from WeatherAPI.

    Sends a request to the external WeatherAPI service and validates
    the response structure.

    Parameters
    ----------
    latitude : float
    longitude : float

    Returns
    -------
    dict
        Raw JSON response containing forecast and current weather data.

    Raises
    ------
    RuntimeError
        If API request fails, response is invalid, or required fields are missing.
    """
    api_key = get_weather_api_key()

    url = "https://api.weatherapi.com/v1/forecast.json"

    params = {
        "key": api_key,
        "q": f"{latitude},{longitude}",
        "days": 1,
        "aqi": "no",
        "alerts": "no",
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise RuntimeError("Weather API request failed") from e

    try:
        data = response.json()
    except ValueError:
        raise RuntimeError("Invalid JSON from Weather API")

    if "current" not in data:
        raise RuntimeError("Invalid Weather API response structure")

    return data
