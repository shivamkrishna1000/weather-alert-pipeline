"""
Database module for storing weather data.
"""

from datetime import UTC, datetime, timedelta


# -------- CLUSTER FUNCTIONS --------
def clean_name(name: str) -> str:
    """
    Normalize a location name by removing suffixes after '-'.

    Parameters
    ----------
    name : str
        Raw name string.

    Returns
    -------
    str
        Cleaned name.
    """
    if not name:
        return ""
    return name.split("-")[0].strip()


def fetch_clusters(connection) -> list[dict]:
    """
    Fetch geographic clusters by aggregating greenhouse coordinates.

    Clusters are grouped by district and taluk, with average
    latitude and longitude computed for each group.

    Returns
    -------
    list
        List of cluster dictionaries.
    """
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            district,
            taluk,
            AVG(latitude) as latitude,
            AVG(longitude) as longitude
        FROM greenhouses
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND district IS NOT NULL
          AND taluk IS NOT NULL
        GROUP BY district, taluk
        """
    )

    rows = cursor.fetchall()
    cursor.close()

    clusters = []

    for row in rows:
        district, taluk, lat, lon = row

        clean_district = clean_name(district)
        clean_taluk = clean_name(taluk)

        cluster_key = f"{clean_district}_{clean_taluk}"

        clusters.append(
            {
                "cluster_key": cluster_key,
                "district": clean_district,
                "taluk": clean_taluk,
                "latitude": lat,
                "longitude": lon,
            }
        )

    return clusters


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


def is_cache_fresh(fetched_at, ttl_hours: int = 6) -> bool:
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
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=UTC)

    return datetime.now(UTC) - fetched_at < timedelta(hours=ttl_hours)


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
