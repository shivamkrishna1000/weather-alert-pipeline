from typing import Dict, List

from app.core.advisory_rules import RULES


def generate_advisories(weather: Dict) -> List[str]:
    """
    Generate weather-based advisories using rule-driven evaluation.

    This function applies category-wise rule evaluation on normalized
    weather features and returns a list of advisory messages.

    Workflow:
    - Evaluate rules independently for each category:
        rain, wind, humidity, temperature
    - Within each category, rules are checked in priority order
      and only the first matching rule is selected
    - Apply conflict resolution:
        - If a rain rule is triggered, temperature rules T1 and T2
          are ignored to avoid irrigation conflicts
        - Temperature rule T3 (low temperature) is still allowed
    - Return advisories ordered by priority:
        rain → wind → humidity → temperature

    Parameters
    ----------
    weather : dict
        Dictionary containing normalized weather features:
        - max_temp : float
        - min_temp : float
        - max_rain : float
        - rain_probability : float
        - rain_hours : int
        - max_humidity : float
        - max_wind : float

    Returns
    -------
    list[str]
        Ordered list of advisory messages. Returns an empty list
        if no rules are triggered.

    Notes
    -----
    - Rule definitions are externalized in `advisory_rules.py`
    - Output is deterministic: same input always produces same output
    - Only one rule per category is selected (first-match principle)
    """
    selected_rules = {}

    # Step 1: Evaluate rules category-wise (first match only)
    for category in ["rain", "wind", "humidity", "temperature"]:
        rules = RULES.get(category, [])

        for rule in rules:
            if rule["condition"](weather):
                selected_rules[category] = rule
                break

    # Step 2: Conflict resolution
    rain_triggered = "rain" in selected_rules

    if rain_triggered and "temperature" in selected_rules:
        temp_rule_id = selected_rules["temperature"]["id"]

        # Remove T1 and T2 if rain is present
        if temp_rule_id in {"T1", "T2"}:
            del selected_rules["temperature"]

    # Step 3: Build ordered advisory list
    ordered_categories = ["rain", "wind", "humidity", "temperature"]

    advisories = [
        selected_rules[category]["message"]
        for category in ordered_categories
        if category in selected_rules
    ]

    return advisories
