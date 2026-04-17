"""
Central constants used across the application.

Includes:
- Field mappings for Zoho CRM data
- Allowed greenhouse status values
"""

ZOHO_FIELDS = {
    "id": "id",
    "name": "Name",
    "farmer": "Farmer",
    "phone_fields": ["Mobile", "Farmer_Mobile_No", "Alternate_Number_1"],
    "status": "Current_GH_Status",
    "latitude": "Latitude",
    "longitude": "Longitude",
    "village": "Village",
    "taluk": "Taluk_Block_Mandal",
    "district": "District",
    "state": "State_UT1",
    "region": "Region",
    "cluster": "Clusterss",
}


ALLOWED_STATUSES = {
    "2. FS taken over and being used",
    "3. FS taken over and not being used",
    "4. Given up / don't know",
}
