"""
Zoho CRM data fetching module.

This module handles:
- Fetching greenhouse data from Zoho CRM
- Filtering records based on status and location validity
"""

import time
from datetime import datetime, timezone

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
from app.constants import ZOHO_FIELDS

# In-memory cache
_access_token = None
_expiry_time = 0


def get_valid_access_token() -> str:
    """
    Retrieve a valid Zoho access token.

    Returns cached token if not expired, otherwise refreshes it.
    """
    global _access_token, _expiry_time

    if _access_token and time.time() < _expiry_time:
        return _access_token

    return refresh_access_token()


def refresh_access_token() -> str:
    """
    Refresh Zoho OAuth access token using refresh token.

    Returns
    -------
    str
        New access token.

    Raises
    ------
    requests.exceptions.RequestException
        If token request fails.
    """
    global _access_token, _expiry_time

    url = f"{get_zoho_accounts_url()}/oauth/v2/token"

    params = {
        "refresh_token": get_zoho_refresh_token(),
        "client_id": get_zoho_client_id(),
        "client_secret": get_zoho_client_secret(),
        "grant_type": "refresh_token",
    }

    try:
        response = requests.post(url, params=params, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise RuntimeError("Zoho token refresh failed") from e

    data = response.json()

    _access_token = data["access_token"]
    expires_in = data.get("expires_in", 3600)

    # Keep buffer to avoid edge expiry issues
    _expiry_time = time.time() + expires_in - 60

    return _access_token


def build_select_fields() -> str:
    """
    Build COQL SELECT clause from ZOHO_FIELDS.

    Flattens field mappings, including nested lists, and ensures
    required system fields are included.

    Returns
    -------
    str
        Comma-separated field names for COQL SELECT clause.
    """
    fields = []

    for value in ZOHO_FIELDS.values():
        if isinstance(value, list):
            fields.extend(value)
        else:
            fields.append(value)

    fields.append("Modified_Time")

    # Remove duplicates
    return ", ".join(set(fields))


def build_coql_query(module: str, where_clause: str, select_clause: str, limit: int, offset: int) -> str:
    """
    Construct COQL query string.

    Parameters
    ----------
    module : str
        Zoho module API name.
    where_clause : str
        Query filter conditions.
    select_clause : str
        Comma-separated fields to retrieve.
    limit : int
        Number of records per request.
    offset : int
        Pagination offset.

    Returns
    -------
    str
        COQL query string.
    """
    return f"""
        select {select_clause}
        from {module}
        where {where_clause}
        order by id asc
        limit {limit} offset {offset}
    """


def execute_coql_query(base_url: str, headers: dict, query: str, offset: int) -> dict:
    """
    Execute COQL query.

    Parameters
    ----------
    base_url : str
        Zoho API base URL.
    headers : dict
        Request headers including authorization.
    query : str
        COQL query string.
    offset : int
        Pagination offset for error context.

    Returns
    -------
    dict
        JSON response from COQL API.

    Raises
    ------
    RuntimeError
        If request fails or response is invalid.
    """
    payload = {"select_query": query}

    try:
        response = requests.post(
            f"{base_url}/crm/v8/coql",
            headers=headers,
            json=payload,
            timeout=10,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"COQL API request failed at offset {offset}") from e

    # Handle 204 No Content (valid case)
    if response.status_code == 204:
        return {"data": [], "info": {"more_records": False}}
    
    # Handle empty response body (unexpected case)
    if not response.text.strip():
        raise RuntimeError(
            f"Empty response from COQL API at offset {offset}. "
            f"Status: {response.status_code}"
        )
    
    try:
        return response.json()
    except ValueError:
        raise RuntimeError(
            f"Invalid JSON from COQL API.\n"
            f"Status: {response.status_code}\n"
            f"Response: {response.text[:500]}"
        )


def fetch_all_greenhouse_data(connection) -> list[dict]:
    """
    Fetch greenhouse records from Zoho CRM using COQL.

    Retrieves records with pagination, applies Modified_Time filter,
    and aggregates results into a single list.

    Parameters
    ----------
    connection : Any
        Database connection used to fetch last sync time.

    Returns
    -------
    list[dict]
        List of greenhouse records.

    Raises
    ------
    RuntimeError
        If COQL API request fails.
    """
    base_url = get_zoho_api_base()
    module = get_zoho_module()
    token = get_valid_access_token()
    last_sync = get_last_sync_time(connection)

    print(f"Using last_sync: {last_sync}")

    headers = {
        "Authorization": f"Zoho-oauthtoken {token}",
        "Content-Type": "application/json",
    }

    where_clause = "id is not null"
    if last_sync:
        dt = datetime.fromisoformat(last_sync)
        formatted_time = dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        formatted_time = formatted_time[:-2] + ":" + formatted_time[-2:]
        
        where_clause += f" and Modified_Time >= '{formatted_time}'"

    select_clause = build_select_fields()

    all_records = []
    offset = 0
    limit = 2000

    while True:
        query = build_coql_query(module, where_clause, select_clause, limit, offset)

        print("COQL Query:", query)

        json_data = execute_coql_query(base_url, headers, query, offset)

        data = json_data.get("data", [])

        print(f"Offset {offset}: fetched {len(data)} records")

        if not data:
            break

        all_records.extend(data)

        if not json_data.get("info", {}).get("more_records"):
            print("No more records.")
            break

        offset += limit

    print(f"Total records fetched: {len(all_records)}")

    return all_records