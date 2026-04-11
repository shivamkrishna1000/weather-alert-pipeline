import requests

from app.config import get_weather_api_key


def fetch_weather_raw(latitude: float, longitude: float) -> dict:
    """
    Fetch raw weather data from WeatherAPI.

    Parameters
    ----------
    latitude : float
    longitude : float

    Returns
    -------
    dict
        Raw JSON response from WeatherAPI

    Raises
    ------
    RuntimeError
        If API request fails
    """

    api_key = get_weather_api_key()

    url = "https://api.weatherapi.com/v1/current.json"

    params = {"key": api_key, "q": f"{latitude},{longitude}"}

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
