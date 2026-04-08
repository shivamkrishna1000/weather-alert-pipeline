from app.repositories.weather_repo import (
    fetch_clusters,
    get_cached_weather,
    is_cache_fresh,
    upsert_weather_cache,
    insert_weather_history,
)
from app.services.weather_service import get_weather


def run_weather_pipeline(connection):
    """
    Execute weather data pipeline.

    Steps:
    - Fetch clusters
    - Check cache
    - Fetch fresh data if needed
    - Store cache + history
    """

    clusters = fetch_clusters(connection)

    print(f"Total clusters: {len(clusters)}")

    processed = 0
    skipped = 0

    for cluster in clusters:
        cluster_key = cluster["cluster_key"]

        # Step 1: Check cache
        cached = get_cached_weather(connection, cluster_key)

        if cached and is_cache_fresh(cached["fetched_at"]):
            print(f"Skipping (fresh cache): {cluster_key}")
            skipped += 1
            continue

        try:
            print(f"Fetching weather: {cluster_key}")

            weather = get_weather(
                cluster["latitude"],
                cluster["longitude"]
            )

            # merge cluster + weather
            enriched_cluster = {
                **cluster,
                **weather
            }

            # Step 2: Update cache
            upsert_weather_cache(connection, enriched_cluster)

            # Step 3: Store history
            insert_weather_history(connection, enriched_cluster)

            processed += 1

        except RuntimeError as e:
            print(f"Weather API failure for {cluster_key}: {e}")
            continue

    print(f"Processed: {processed}")
    print(f"Skipped (cache): {skipped}")