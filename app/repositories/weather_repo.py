"""
Database module for storing weather data.
"""

from datetime import UTC, datetime

from app.config import get_cluster_mode
from app.services.cluster_service import build_cluster_key, build_distance_clusters


# -------- CLUSTER FUNCTIONS --------
def fetch_clusters(connection):
    """
    Fetch geographic clusters by aggregating greenhouse coordinates.

    Clusters are grouped by district and taluk/village, with average
    latitude and longitude computed for each group.

    Returns
    -------
    list
        List of cluster dictionaries.
    """
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT district, taluk, village, latitude, longitude
        FROM greenhouses
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """
    )

    rows = cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    records = [dict(zip(columns, row)) for row in rows]

    mode = get_cluster_mode()

    # 🔹 DISTANCE MODE
    if mode == "distance":
        return build_distance_clusters(records)

    # 🔹 TALUK / VILLAGE MODE
    clusters = {}

    for r in records:
        key = build_cluster_key(r)

        if key not in clusters:
            clusters[key] = {"latitude": [], "longitude": []}

        clusters[key]["latitude"].append(r["latitude"])
        clusters[key]["longitude"].append(r["longitude"])

    final_clusters = []

    for key, values in clusters.items():
        lat = sum(values["latitude"]) / len(values["latitude"])
        lon = sum(values["longitude"]) / len(values["longitude"])

        final_clusters.append({"cluster_key": key, "latitude": lat, "longitude": lon})

    return final_clusters


# -------- CACHE FUNCTIONS --------
def get_cached_weather(connection, cluster_key: str) -> dict | None:
    """
    Retrieve cached weather data for a cluster.

    Parameters
    ----------
    cluster_key : str
        Unique identifier for the cluster.

    Returns
    -------
    dict or None
        Cached weather data if present.
    """
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            max_temp, min_temp, max_rain,
            rain_probability, rain_hours,
            max_humidity, max_wind,
            fetched_at
        FROM weather_cache
        WHERE cluster_key = %s
        """,
        (cluster_key,),
    )

    row = cursor.fetchone()
    cursor.close()

    if not row:
        return None

    (
        max_temp,
        min_temp,
        max_rain,
        rain_probability,
        rain_hours,
        max_humidity,
        max_wind,
        fetched_at,
    ) = row

    return {
        "max_temp": max_temp,
        "min_temp": min_temp,
        "max_rain": max_rain,
        "rain_probability": rain_probability,
        "rain_hours": rain_hours,
        "max_humidity": max_humidity,
        "max_wind": max_wind,
        "fetched_at": fetched_at,
    }


def is_cache_fresh(fetched_at) -> bool:
    """
    Check if cached weather data is still valid.

    Parameters
    ----------
    fetched_at : datetime
        Timestamp of last fetch.
    ttl_hours : int
        Cache validity duration.

    Returns
    -------
    bool
    """
    if not fetched_at:
        return False

    # Ensure timezone-aware
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=UTC)

    now = datetime.now(UTC)

    return fetched_at.date() == now.date()


# -------- WRITE FUNCTIONS --------
def upsert_weather_cache(connection, cluster: dict) -> None:
    """
    Insert or update weather cache for a cluster.

    Parameters
    ----------
    cluster : dict
        Cluster data with weather fields.
    """
    cursor = connection.cursor()

    cursor.execute(
        """
        INSERT INTO weather_cache (
            cluster_key, latitude, longitude,

            max_temp, min_temp, max_rain,
            rain_probability, rain_hours,
            max_humidity, max_wind,

            fetched_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (cluster_key) DO UPDATE SET
            max_temp = EXCLUDED.max_temp,
            min_temp = EXCLUDED.min_temp,
            max_rain = EXCLUDED.max_rain,
            rain_probability = EXCLUDED.rain_probability,
            rain_hours = EXCLUDED.rain_hours,
            max_humidity = EXCLUDED.max_humidity,
            max_wind = EXCLUDED.max_wind,
            fetched_at = NOW()
        """,
        (
            cluster["cluster_key"],
            cluster["latitude"],
            cluster["longitude"],
            cluster["max_temp"],
            cluster["min_temp"],
            cluster["max_rain"],
            cluster["rain_probability"],
            cluster["rain_hours"],
            cluster["max_humidity"],
            cluster["max_wind"],
        ),
    )

    connection.commit()
    cursor.close()


def insert_weather_history(connection, cluster: dict) -> None:
    """
    Insert historical weather record for a cluster.

    Parameters
    ----------
    cluster : dict
        Cluster data with weather fields.
    """
    cursor = connection.cursor()

    cursor.execute(
        """
        INSERT INTO weather_data (
            cluster_key, latitude, longitude,

            max_temp, min_temp, max_rain,
            rain_probability, rain_hours,
            max_humidity, max_wind,

            fetched_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """,
        (
            cluster["cluster_key"],
            cluster["latitude"],
            cluster["longitude"],
            cluster["max_temp"],
            cluster["min_temp"],
            cluster["max_rain"],
            cluster["rain_probability"],
            cluster["rain_hours"],
            cluster["max_humidity"],
            cluster["max_wind"],
        ),
    )

    connection.commit()
    cursor.close()
