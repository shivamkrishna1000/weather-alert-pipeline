from app.config import is_debug_mode
from app.repositories.advisory_repo import (
    fetch_pending_advisories,
    mark_advisories_as_sent,
)
from app.services.delivery_service import (
    format_greenhouse_message,
    group_advisories_by_farmer,
)
from app.services.wati_service import send_whatsapp_message


def run_delivery_pipeline(connection) -> None:
    """
    Execute advisory delivery pipeline.

    Workflow:
    - Fetch pending advisories
    - Group by farmer
    - Format messages
    - Send messages (or print in debug mode)

    Parameters
    ----------
    connection : Any

    Returns
    -------
    None
    """

    records = fetch_pending_advisories(connection)

    if not records:
        print("No pending advisories to send.")
        return

    grouped = group_advisories_by_farmer(records)

    print(f"Total farmers to notify: {len(grouped)}")

    for phone, data in grouped.items():
        farmer_name = data["farmer_name"]

        all_success = True

        for gh_name, advisories in data["greenhouses"].items():
            message = format_greenhouse_message(gh_name, advisories)

            print(f"Sending for greenhouse: {gh_name}")

            success = send_whatsapp_message(
                phone=phone,
                farmer_name=farmer_name,
                message=message,
            )

            if not success:
                all_success = False

        if all_success:
            print(f"All messages sent successfully to {phone}")

            if not is_debug_mode():
                mark_advisories_as_sent(connection, data["ids"])
        else:
            print(f"Some messages failed for {phone}")
