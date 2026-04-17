from app.repositories.weather_repo import (
    fetch_clusters,
    get_cached_weather,
    insert_weather_history,
    is_cache_fresh,
    upsert_weather_cache,
)
from app.services.weather_service import get_weather


def run_weather_pipeline(connection) -> None:
    """
    Execute the weather data pipeline for all clusters.

    This pipeline performs the following steps for each cluster:
    - Retrieve cluster metadata from database
    - Check if cached weather data is still fresh
    - Fetch new weather data if cache is stale or missing
    - Update weather cache and append to historical records

    Parameters
    ----------
    connection : Any
        Active database connection used for all read/write operations.

    Returns
    -------
    None

    Raises
    ------
    RuntimeError
        Propagated if weather API calls fail unexpectedly.

    Notes
    -----
    - Uses caching to reduce external API calls.
    - Failures for individual clusters do not stop the pipeline.
    - Prints processing summary (processed vs skipped).
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

            weather = get_weather(cluster["latitude"], cluster["longitude"])

            # merge cluster + weather
            enriched_cluster = {**cluster, **weather}

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
