from app.external.weather_client import fetch_weather_raw


def normalize_weather(data: dict) -> dict:
    """
    Extract structured weather features from raw forecast data.

    Processes the first day's forecast and derives key metrics
    such as temperature, rainfall, humidity, and wind.

    Parameters
    ----------
    data : dict
        Raw JSON response from WeatherAPI. Must contain:
        data["forecast"]["forecastday"][0]

    Returns
    -------
    dict
        Dictionary containing:
        - max_temp : float
        - min_temp : float
        - max_rain : float
        - rain_probability : float
        - rain_hours : int
        - max_humidity : float
        - max_wind : float

    Raises
    ------
    RuntimeError
        If hourly forecast data is missing or invalid.
    """
    day_data = data["forecast"]["forecastday"][0]
    hours = day_data["hour"]

    if not hours:
        raise RuntimeError("No hourly forecast data available")

    # -------- Temperature --------
    max_temp = day_data["day"]["maxtemp_c"]
    min_temp = day_data["day"]["mintemp_c"]

    # -------- Rain --------
    max_rain = max(h["precip_mm"] for h in hours)
    rain_probability = day_data["day"]["daily_chance_of_rain"]
    rain_hours = sum(1 for h in hours if h["will_it_rain"] == 1)

    # -------- Humidity --------
    max_humidity = max(h["humidity"] for h in hours)

    # -------- Wind --------
    max_wind = day_data["day"]["maxwind_kph"]

    return {
        "max_temp": max_temp,
        "min_temp": min_temp,
        "max_rain": max_rain,
        "rain_probability": rain_probability,
        "rain_hours": rain_hours,
        "max_humidity": max_humidity,
        "max_wind": max_wind,
    }


def get_weather(latitude: float, longitude: float) -> dict:
    """
    Fetch and process weather data for a given location.

    This function retrieves raw weather data from the external API
    and transforms it into structured features used for downstream
    storage and advisory generation.

    Parameters
    ----------
    latitude : float
    longitude : float

    Returns
    -------
    dict
        Normalized weather features including temperature, rainfall,
        humidity, and wind metrics.

    Raises
    ------
    RuntimeError
        If API request fails or response structure is invalid.
    """
    raw_data = fetch_weather_raw(latitude, longitude)
    forecast_features = normalize_weather(raw_data)

    return forecast_features
