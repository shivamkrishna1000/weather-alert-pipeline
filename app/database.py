"""
Database module for storing greenhouse data.
"""

import psycopg2


def get_connection(database_url: str):
    """Create PostgreSQL connection (Neon)."""
    return psycopg2.connect(database_url)


def create_tables(connection) -> None:
    """
    Create required database tables if they do not exist.

    This includes greenhouse, cache, metadata, and weather tables.
    """
    cursor = connection.cursor()

    # Table with location
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS greenhouses (
            id TEXT PRIMARY KEY,
            name TEXT,
            farmer_name TEXT,
            phone TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            district TEXT,
            taluk TEXT,
            status TEXT,
            geocoded BOOLEAN DEFAULT FALSE
        )
        """
    )

    # Table without location
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS greenhouses_missing_location (
            id TEXT PRIMARY KEY,
            name TEXT,
            farmer_name TEXT,
            phone TEXT,
            status TEXT,
            village TEXT,
            taluk TEXT,
            district TEXT,
            state TEXT,
            region TEXT,
            cluster TEXT,
            attempts INTEGER DEFAULT 0
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS geocode_cache (
            address TEXT PRIMARY KEY,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS weather_cache (
            cluster_key TEXT PRIMARY KEY,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            temperature FLOAT,
            rainfall FLOAT,
            humidity FLOAT,
            wind_speed FLOAT,
            fetched_at TIMESTAMP
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS weather_data (
            id SERIAL PRIMARY KEY,
            cluster_key TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            temperature FLOAT,
            rainfall FLOAT,
            humidity FLOAT,
            wind_speed FLOAT,
            fetched_at TIMESTAMP
        )
        """
    )

    connection.commit()
    cursor.close()


def update_last_sync_time(connection, timestamp: str) -> None:
    """
    Update the last synchronization timestamp.

    Parameters
    ----------
    timestamp : str
        ISO formatted timestamp.
    """
    cursor = connection.cursor()

    cursor.execute(
        """
        INSERT INTO sync_metadata (key, value)
        VALUES ('last_sync', %s)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """,
        (timestamp,),
    )

    connection.commit()
    cursor.close()
