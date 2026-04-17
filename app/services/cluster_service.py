from collections import defaultdict

import numpy as np
from sklearn.cluster import DBSCAN

from app.config import get_cluster_mode


def clean_name(name: str) -> str:
    """
    Normalize location name by removing suffixes after '-'.

    This helps standardize inconsistent naming formats
    (e.g., 'Village-A' → 'Village') before clustering.

    Parameters
    ----------
    name : str
        Raw location name.

    Returns
    -------
    str
        Cleaned location name. Returns empty string if input is None or empty.
    """
    if not name:
        return ""
    return name.split("-")[0].strip()


def build_cluster_key(record: dict) -> str:
    """
    Generate a cluster key based on configured clustering mode.

    The cluster key groups greenhouse records by geographic hierarchy:
    - taluk mode → district + taluk
    - village mode → district + taluk + village

    Parameters
    ----------
    record : dict
        Greenhouse record containing location fields:
        'district', 'taluk', and 'village'.

    Returns
    -------
    str or None
        Cluster key string based on selected mode.
        Returns None if clustering mode is invalid.
    """
    mode = get_cluster_mode()

    district = clean_name(record.get("district"))
    taluk = clean_name(record.get("taluk"))
    village = clean_name(record.get("village"))

    if mode == "taluk":
        return f"taluk_{district}_{taluk}"

    elif mode == "village":
        return f"village_{district}_{taluk}_{village}"

    else:
        return None


def build_distance_clusters(records: list[dict], radius_km: float = 3.0):
    """
    Cluster records based on geographic proximity using DBSCAN.

    Applies DBSCAN clustering with haversine distance to group
    nearby coordinates within a specified radius.

    Parameters
    ----------
    records : list[dict]
        List of greenhouse records with 'latitude' and 'longitude'.
    radius_km : float, optional
        Maximum distance (in kilometers) between points in a cluster.

    Returns
    -------
    list[dict]
        List of clusters, each containing:
        - cluster_key : str
        - latitude : float (cluster centroid)
        - longitude : float (cluster centroid)
    """
    coords = np.array([[r["latitude"], r["longitude"]] for r in records])

    coords_rad = np.radians(coords)

    kms_per_radian = 6371.0088
    epsilon = radius_km / kms_per_radian

    db = DBSCAN(
        eps=epsilon, min_samples=1, algorithm="ball_tree", metric="haversine"
    ).fit(coords_rad)

    labels = db.labels_

    clusters = defaultdict(list)

    for label, record in zip(labels, records):
        clusters[label].append(record)

    final_clusters = []

    for label, group in clusters.items():
        lat = sum(r["latitude"] for r in group) / len(group)
        lon = sum(r["longitude"] for r in group) / len(group)

        lat_r = round(lat, 3)
        lon_r = round(lon, 3)

        final_clusters.append(
            {
                "cluster_key": f"distance_{lat_r}_{lon_r}",
                "latitude": lat,
                "longitude": lon,
            }
        )

    return final_clusters
