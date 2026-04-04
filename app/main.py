"""
Entry point for greenhouse data pipeline.
"""

from app.config import get_database_url, load_environment
from app.database import get_connection
from app.pipelines.geocode_pipeline import run_geocode_pipeline
from app.pipelines.sync_pipeline import run_sync_pipeline


def main():
    load_environment()

    connection = None

    try:
        # 🔹 Step 1: Connect to DB
        database_url = get_database_url()
        connection = get_connection(database_url)

        print("Database connected.")

        # 🔹 Step 2: Sync Zoho data
        print("Starting greenhouse sync...")
        run_sync_pipeline(connection)
        print("Greenhouse sync completed.")

        # 🔹 Step 3: Run geocoding
        print("Starting geocoding pipeline...")
        run_geocode_pipeline(connection, batch_size=100)
        print("Geocoding completed.")

    except Exception as e:
        print(f"Error occurred: {e}")

    finally:
        if connection:
            connection.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()
