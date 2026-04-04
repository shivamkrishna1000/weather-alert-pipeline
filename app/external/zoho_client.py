"""
Zoho CRM data fetching module.

This module handles:
- Fetching greenhouse data from Zoho CRM
- Filtering records based on status and location validity
"""

import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests

from app.config import (
    get_zoho_accounts_url,
    get_zoho_api_base,
    get_zoho_client_id,
    get_zoho_client_secret,
    get_zoho_module,
    get_zoho_refresh_token,
)
from app.repositories.greenhouse_repo import get_last_sync_time

# In-memory cache
_access_token = None
_expiry_time = 0


def get_valid_access_token():
    global _access_token, _expiry_time

    if _access_token and time.time() < _expiry_time:
        return _access_token

    return refresh_access_token()


def refresh_access_token():
    global _access_token, _expiry_time

    url = f"{get_zoho_accounts_url()}/oauth/v2/token"

    params = {
        "refresh_token": get_zoho_refresh_token(),
        "client_id": get_zoho_client_id(),
        "client_secret": get_zoho_client_secret(),
        "grant_type": "refresh_token",
    }

    response = requests.post(url, params=params, timeout=10)
    response.raise_for_status()

    data = response.json()

    _access_token = data["access_token"]
    expires_in = data.get("expires_in", 3600)

    # Keep buffer to avoid edge expiry issues
    _expiry_time = time.time() + expires_in - 60

    return _access_token


def to_rfc1123(iso_time: str) -> str:
    """
    Convert ISO datetime string to RFC1123 format required by Zoho API.

    Parameters
    ----------
    iso_time : str
        ISO formatted datetime string.

    Returns
    -------
    str
        RFC1123 formatted datetime string.
    """
    dt = datetime.fromisoformat(iso_time)
    return dt.astimezone(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")


def fetch_all_greenhouse_data(connection, per_page: int = 200) -> List[Dict[str, Any]]:
    """
    Fetch greenhouse records from Zoho CRM using pagination.

    Parameters
    ----------
    connection : Any
        Database connection used to fetch last sync time.
    per_page : int, optional
        Number of records per page (default is 200).

    Returns
    -------
    List[Dict[str, Any]]
        List of greenhouse records retrieved from Zoho API.

    Raises
    ------
    requests.exceptions.RequestException
        If API request fails.
    """
    base_url = get_zoho_api_base()
    module = get_zoho_module()
    token = get_valid_access_token()
    last_sync = get_last_sync_time(connection)
    print(f"Using last_sync: {last_sync}")

    headers = {
        "Authorization": f"Zoho-oauthtoken {token}",
    }
    if last_sync:
        headers["If-Modified-Since"] = to_rfc1123(last_sync)

    all_records: List[Dict[str, Any]] = []
    page = 1
    latest_modified_time = None

    while True:
        response = fetch_page(base_url, module, headers, page, per_page)
        data = extract_records(response)

        for record in data:
            mod_time = record.get("Modified_Time")

            if mod_time:
                if not latest_modified_time or mod_time > latest_modified_time:
                    latest_modified_time = mod_time

        print(f"Page {page} fetched: {len(data)} records")

        if not data:
            break

        all_records.extend(data)
        if len(data) < per_page:
            print(f"Last page reached at page {page}")
            break
        page += 1

    print(f"Total records fetched: {len(all_records)}")

    return all_records


def fetch_page(base_url, module, headers, page, per_page):
    """
    Fetch a single page of greenhouse records from Zoho CRM.

    Parameters
    ----------
    base_url : str
        Base URL for Zoho API.
    module : str
        Zoho module name.
    headers : Dict
        HTTP headers including authorization.
    page : int
        Page number to fetch.
    per_page : int
        Number of records per page.

    Returns
    -------
    requests.Response
        HTTP response object from Zoho API.

    Raises
    ------
    RuntimeError
        If API request fails.
    """
    params = {
        "per_page": per_page,
        "page": page,
    }

    try:
        response = requests.get(
            f"{base_url}/crm/v2/{module}",
            headers=headers,
            params=params,
            timeout=10,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Zoho API request failed on page {page}") from e

    return response


def extract_records(response):
    """
    Extract greenhouse records from Zoho API response.

    Parameters
    ----------
    response : requests.Response
        Response object returned from Zoho API.

    Returns
    -------
    List[Dict[str, Any]]
        List of greenhouse records.

    Raises
    ------
    RuntimeError
        If response contains invalid JSON.
    """
    if not response.text.strip():
        return []

    try:
        json_data = response.json()
    except ValueError as e:
        raise RuntimeError("Invalid JSON from Zoho API") from e

    return json_data.get("data", [])
