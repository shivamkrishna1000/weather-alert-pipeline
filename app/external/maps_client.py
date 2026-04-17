import requests

from app.config import get_google_maps_api_key


def geocode_address(address: str) -> tuple[float, float]:
    """
    Fetch geographic coordinates for a given address using Google Maps API.

    Parameters
    ----------
    address : str
        Address string to geocode.

    Returns
    -------
    tuple[float, float]
        Latitude and longitude of the resolved address.

    Raises
    ------
    ValueError
        If the API returns a non-success status (invalid address).
    RuntimeError
        If the API request fails or response is invalid.
    """
    api_key = get_google_maps_api_key()

    url = "https://maps.googleapis.com/maps/api/geocode/json"

    params = {
        "address": address,
        "key": api_key,
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"API request failed for address: {address}") from e

    try:
        data = response.json()
    except ValueError as e:
        raise RuntimeError("Invalid JSON response from geocoding API") from e

    if data.get("status") != "OK":
        raise ValueError(f"Geocoding failed for address: {address}")

    location = data["results"][0]["geometry"]["location"]

    return location["lat"], location["lng"]
