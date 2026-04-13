from app.external.weather_client import fetch_weather_raw


def normalize_weather(data: dict) -> dict:
    """
    Extract actionable weather features from forecast data (next 12 hours).

        Returns
    -------
    dict
        {
            max_temp,
            min_temp,
            max_rain,
            rain_probability,
            rain_hours,
            max_humidity,
            max_wind
        }
    """

    hours = data["forecast"]["forecastday"][0]["hour"]

    # Get current time
    current_time = data["location"]["localtime"]

    # Filter next 12 hours
    next_hours = []

    for h in hours:
        if h["time"] >= current_time:
            next_hours.append(h)

    next_hours = next_hours[:12]

    if not next_hours:
        raise RuntimeError("No forecast data available for next 12 hours")

    # -------- Feature Extraction --------
    max_temp = max(h["temp_c"] for h in next_hours)
    min_temp = min(h["temp_c"] for h in next_hours)

    max_rain = max(h["precip_mm"] for h in next_hours)
    rain_probability = max(h["chance_of_rain"] for h in next_hours)
    rain_hours = sum(1 for h in next_hours if h["will_it_rain"] == 1)

    max_humidity = max(h["humidity"] for h in next_hours)
    max_wind = max(h["wind_kph"] for h in next_hours)

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
    Fetch weather and return both:
    - DB-compatible fields (old)
    - Advisory features (new)

        Returns
    -------
    dict
        Normalized weather data
    """

    raw_data = fetch_weather_raw(latitude, longitude)
    forecast_features = normalize_weather(raw_data)

    return forecast_features
