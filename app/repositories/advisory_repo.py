from datetime import date


def fetch_greenhouses_by_cluster(connection, cluster_key: str) -> list[dict]:
    """
    Fetch greenhouse records associated with a given cluster.

    This function retrieves farmer-level greenhouse data using the
    persisted `cluster_key` assigned during the clustering phase.
    It supports all clustering modes (taluk, village, distance)
    by relying on stored cluster membership rather than recomputing it.

    Parameters
    ----------
    connection : Any
        Active database connection.
    cluster_key : str
        Cluster identifier assigned to greenhouses.

    Returns
    -------
    list[dict]
        List of greenhouse records with keys:
        - id : str
        - farmer_name : str
        - phone : str

    Notes
    -----
    - Requires `cluster_key` to be populated in the `greenhouses` table.
    - Cluster assignment must be completed before invoking this function.
    - Works consistently across all clustering modes, including
      distance-based clustering.
    - Returns an empty list if no greenhouses are mapped to the cluster.
    """
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT id, farmer_name, phone
        FROM greenhouses
        WHERE cluster_key = %s
        """,
        (cluster_key,),
    )

    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    cursor.close()

    return [dict(zip(columns, row)) for row in rows]


def advisory_already_sent(connection, greenhouse_id: str, advisory: str) -> bool:
    """
    Check if an advisory has already been sent to a greenhouse today.

    Parameters
    ----------
    connection : Any
        Database connection.
    greenhouse_id : str
        Unique greenhouse identifier.
    advisory : str
        Advisory message.

    Returns
    -------
    bool
        True if advisory already exists for today, else False.
    """
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT 1 FROM advisory_logs
        WHERE greenhouse_id = %s
        AND advisory = %s
        AND advisory_date = %s
        LIMIT 1
        """,
        (greenhouse_id, advisory, date.today()),
    )

    exists = cursor.fetchone() is not None
    cursor.close()
    return exists


def insert_advisory_log(
    connection,
    greenhouse: dict,
    cluster_key: str,
    advisory: str,
) -> None:
    """
    Insert advisory delivery log for a greenhouse.

    Parameters
    ----------
    connection : Any
        Database connection.
    greenhouse : dict
        Greenhouse record containing id, farmer_name, phone.
    cluster_key : str
        Cluster identifier.
    advisory : str
        Advisory message.

    Returns
    -------
    None
    """
    cursor = connection.cursor()

    cursor.execute(
        """
        INSERT INTO advisory_logs (
            greenhouse_id,
            farmer_name,
            phone,
            cluster_key,
            advisory,
            advisory_date,
            delivery_status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (greenhouse_id, advisory, advisory_date)
        DO NOTHING
        """,
        (
            greenhouse["id"],
            greenhouse["farmer_name"],
            greenhouse["phone"],
            cluster_key,
            advisory,
            date.today(),
            "pending",
        ),
    )

    connection.commit()
    cursor.close()
