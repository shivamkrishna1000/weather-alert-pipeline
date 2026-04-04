import pytest


def test_get_database_url_missing(monkeypatch):
    from app.config import get_database_url

    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(ValueError):
        get_database_url()


def test_get_google_maps_api_key_missing(monkeypatch):
    from app.config import get_google_maps_api_key

    monkeypatch.delenv("GOOGLE_MAPS_API_KEY", raising=False)

    with pytest.raises(ValueError):
        get_google_maps_api_key()
