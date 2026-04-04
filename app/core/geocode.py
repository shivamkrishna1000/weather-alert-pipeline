from typing import Dict, Optional


def build_address(record: Dict) -> Optional[str]:
    """
    Construct a normalized address string from greenhouse record fields.

    Parameters
    ----------
    record : Dict
        Greenhouse record containing address components.

    Returns
    -------
    Optional[str]
        Cleaned address string or None if insufficient data is present.
    """
    parts = [
        record.get("village"),
        record.get("taluk"),
        record.get("district"),
        record.get("state"),
    ]

    parts = [p for p in parts if p]

    if not parts:
        return None

    cleaned_parts = []

    for p in parts:
        sub_parts = p.replace("-", ", ").split(",")

        for sp in sub_parts:
            sp = sp.strip()
            if sp and sp not in cleaned_parts:
                cleaned_parts.append(sp)

    return ", ".join(cleaned_parts)
