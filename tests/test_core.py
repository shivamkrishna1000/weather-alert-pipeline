from app.constants import ALLOWED_STATUSES, ZOHO_FIELDS
from app.core.geocode import build_address
from app.core.greenhouse import extract_fields, filter_greenhouses

# ------------------ GREENHOUSE ------------------


def test_filter_greenhouses_valid_records():
    records = [
        {
            ZOHO_FIELDS["status"]: list(ALLOWED_STATUSES)[0],
            ZOHO_FIELDS["latitude"]: 17.1,
            ZOHO_FIELDS["longitude"]: 78.1,
        },
        {
            ZOHO_FIELDS["status"]: "invalid",
            ZOHO_FIELDS["latitude"]: 17.1,
            ZOHO_FIELDS["longitude"]: 78.1,
        },
    ]

    result = filter_greenhouses(records, ALLOWED_STATUSES, ZOHO_FIELDS)

    assert len(result) == 1


def test_extract_fields_structure():
    records = [
        {
            ZOHO_FIELDS["name"]: "GH1",
            ZOHO_FIELDS["farmer"]: {"name": "Ravi"},
            "Mobile": "999",
            "Farmer_Mobile_No": None,
            "Alternate_Number_1": None,
            ZOHO_FIELDS["latitude"]: 17.1,
            ZOHO_FIELDS["longitude"]: 78.1,
            ZOHO_FIELDS["status"]: "active",
            ZOHO_FIELDS["id"]: "1",
        }
    ]

    result = extract_fields(records, ZOHO_FIELDS)

    assert result[0]["greenhouse_name"] == "GH1"
    assert result[0]["farmer_name"] == "Ravi"
    assert result[0]["phone"] == "999"


def test_phone_fallback_priority():
    records = [
        {
            "Name": "GH1",
            "Farmer": {"name": "Ravi"},
            "Mobile": None,
            "Farmer_Mobile_No": None,
            "Alternate_Number_1": "888",
            "Latitude": 17.1,
            "Longitude": 78.1,
            "Current_GH_Status": "active",
            "id": "1",
        }
    ]

    result = extract_fields(records, ZOHO_FIELDS)

    assert result[0]["phone"] == "888"


def test_build_address_dedup_and_cleaning():
    record = {
        "village": "Chitawa-Chitawa",
        "taluk": "Kuchaman, Kuchaman",
        "district": "Nagaur",
        "state": "Rajasthan",
    }

    result = build_address(record)

    assert result == "Chitawa, Kuchaman, Nagaur, Rajasthan"


def test_get_phone_priority_and_cleanup():
    record = {
        "Mobile": " 999  ",
        "Farmer_Mobile_No": "888",
        "Alternate_Number_1": "777",
    }

    result = extract_fields(
        [
            record
            | {
                "Name": "GH",
                "Farmer": {"name": "Ravi"},
                "Latitude": 1,
                "Longitude": 2,
                "Current_GH_Status": "active",
                "id": "1",
            }
        ],
        ZOHO_FIELDS,
    )

    assert result[0]["phone"] == "999"


# ------------------ GEOCODE ------------------


def test_build_address_basic():
    record = {
        "village": "Chitawa",
        "taluk": "Kuchaman",
        "district": "Nagaur",
        "state": "Rajasthan",
    }

    result = build_address(record)

    assert result == "Chitawa, Kuchaman, Nagaur, Rajasthan"


def test_build_address_none():
    assert build_address({}) is None
