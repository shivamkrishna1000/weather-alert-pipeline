"""
Configuration loader module.
"""

import os

from dotenv import load_dotenv


def load_environment() -> None:
    """Load environment variables from .env file."""
    load_dotenv()


def get_zoho_client_id() -> str:
    """Return Zoho client id."""
    val = os.environ.get("ZOHO_CLIENT_ID")
    if not val:
        raise ValueError("ZOHO_CLIENT_ID is not set")
    return val


def get_zoho_client_secret() -> str:
    """Return Zoho client secret."""
    val = os.environ.get("ZOHO_CLIENT_SECRET")
    if not val:
        raise ValueError("ZOHO_CLIENT_SECRET is not set")
    return val


def get_zoho_refresh_token() -> str:
    """Return Zoho refresh token."""
    val = os.environ.get("ZOHO_REFRESH_TOKEN")
    if not val:
        raise ValueError("ZOHO_REFRESH_TOKEN is not set")
    return val


def get_zoho_accounts_url() -> str:
    """Return Zoho account URL."""
    return os.environ.get("ZOHO_ACCOUNTS_URL", "https://accounts.zoho.com")


def get_zoho_api_base() -> str:
    """Return Zoho API base URL."""
    return os.environ.get("ZOHO_API_BASE", "https://www.zohoapis.com")


def get_zoho_module() -> str:
    """Return Zoho module name."""
    return os.environ.get("ZOHO_MODULE", "Greenhouse")


def get_google_maps_api_key() -> str:
    """Return Google Maps API key."""
    key = os.environ.get("GOOGLE_MAPS_API_KEY")
    if not key:
        raise ValueError("GOOGLE_MAPS_API_KEY is not set")
    return key


def get_database_url() -> str:
    """Return database URL."""
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL is not set")
    return db_url


def get_test_database_url() -> str:
    """Return test database URL."""
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        raise ValueError("TEST_DATABASE_URL is not set")
    return url


def get_weather_api_key() -> str:
    """Return Weather API key."""
    key = os.environ.get("WEATHER_API_KEY")
    if not key:
        raise ValueError("WEATHER_API_KEY is not set")
    return key
