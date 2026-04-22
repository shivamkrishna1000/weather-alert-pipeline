from app.repositories.advisory_repo import (
    advisory_already_sent,
    fetch_greenhouses_by_cluster,
    insert_advisory_log,
)
from app.repositories.weather_repo import (
    fetch_clusters,
    get_cached_weather,
    insert_weather_history,
    is_cache_fresh,
    upsert_weather_cache,
)
from app.services.advisory_service import generate_advisories
from app.services.weather_service import get_weather


def run_weather_pipeline(connection) -> None:
    """
    Execute weather pipeline for all clusters.

    Parameters
    ----------
    connection : Any

    Returns
    -------
    None
    """
    clusters = fetch_clusters(connection)

    if not clusters:
        print("No clusters available. Skipping weather pipeline.")
        return

    print(f"Total clusters: {len(clusters)}")

    processed = 0
    skipped = 0

    for cluster in clusters:
        try:
            result = process_cluster(connection, cluster)

            if result:
                processed += 1
            else:
                skipped += 1

        except RuntimeError as e:
            print(f"Weather API failure for {cluster['cluster_key']}: {e}")
            continue

    print(f"Processed: {processed}")
    print(f"Skipped (cache): {skipped}")


def should_skip_cluster(connection, cluster_key: str) -> bool:
    """
    Determine whether weather fetch should be skipped due to fresh cache.

    Parameters
    ----------
    connection : Any
    cluster_key : str

    Returns
    -------
    bool
    """
    cached = get_cached_weather(connection, cluster_key)

    if cached and is_cache_fresh(cached["fetched_at"]):
        print(f"Skipping (fresh cache): {cluster_key}")
        return True

    return False


def fetch_and_prepare_weather(cluster: dict) -> dict:
    """
    Fetch weather data and enrich cluster with features and advisories.

    Parameters
    ----------
    cluster : dict

    Returns
    -------
    dict
        Enriched cluster with weather + advisories
    """
    print(f"Fetching weather: {cluster['cluster_key']}")

    weather = get_weather(cluster["latitude"], cluster["longitude"])

    advisories = generate_advisories(weather)

    enriched = {**cluster, **weather, "advisories": advisories}

    print(f"Advisories for {cluster['cluster_key']}: {advisories}")

    return enriched


def generate_and_store_advisories(
    connection, cluster_key: str, advisories: list[str]
) -> None:
    """
    Generate and persist advisory logs for all greenhouses in a cluster.

    Parameters
    ----------
    connection : Any
    cluster_key : str
    advisories : list[str]

    Returns
    -------
    None
    """
    greenhouses = fetch_greenhouses_by_cluster(connection, cluster_key)

    for gh in greenhouses:
        for advisory in advisories:
            if advisory_already_sent(connection, gh["id"], advisory):
                continue

            insert_advisory_log(connection, gh, cluster_key, advisory)


def update_weather_storage(connection, cluster: dict) -> None:
    """
    Persist weather data into cache and historical storage.

    Parameters
    ----------
    connection : Any
    cluster : dict

    Returns
    -------
    None
    """
    upsert_weather_cache(connection, cluster)
    insert_weather_history(connection, cluster)


def process_cluster(connection, cluster: dict) -> bool:
    """
    Process a single cluster end-to-end.

    Parameters
    ----------
    connection : Any
    cluster : dict

    Returns
    -------
    bool
        True if processed, False if skipped
    """
    cluster_key = cluster["cluster_key"]

    if should_skip_cluster(connection, cluster_key):
        return False

    enriched = fetch_and_prepare_weather(cluster)

    generate_and_store_advisories(
        connection,
        cluster_key,
        enriched["advisories"],
    )

    update_weather_storage(connection, enriched)

    return True
