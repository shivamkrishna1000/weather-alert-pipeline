import requests

from app.config import (
    get_wati_api_token,
    get_wati_base_url,
    get_wati_template_name,
    is_debug_mode,
)


def send_whatsapp_message(phone: str, farmer_name: str, message: str) -> bool:
    """
    Send WhatsApp template message via WATI.

    This function handles:
    - Debug mode (prints instead of sending)
    - API request construction
    - Error handling and response validation

    Parameters
    ----------
    phone : str
        Farmer phone number (must include country code, e.g., 91XXXXXXXXXX)
    farmer_name : str
        Name of the farmer (mapped to template variable {{1}})
    message : str
        Formatted advisory message block (mapped to template variable {{2}})

    Returns
    -------
    bool
        True if message sent successfully, False otherwise.
    """

    # -------- DEBUG MODE --------
    if is_debug_mode():
        print("\n[DEBUG MODE] Message NOT sent")
        print(f"Phone: {phone}")
        print(f"Farmer: {farmer_name}")
        print("Message:")
        print(message)
        print("-" * 50)
        return False  # Important: treat as NOT sent

    # -------- BUILD REQUEST --------
    base_url = get_wati_base_url()
    token = get_wati_api_token()
    template_name = get_wati_template_name()

    url = f"{base_url}/api/v1/sendTemplateMessage?whatsappNumber={phone}"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    payload = {
        "template_name": template_name,
        "broadcast_name": "weather_alert",
        "parameters": [
            {"name": "1", "value": farmer_name},
            {"name": "2", "value": message},
        ],
    }

    # -------- API CALL --------
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        print("\n--- WATI DEBUG ---")
        print("STATUS:", response.status_code)
        print("RESPONSE:", response.text)
        print("------------------\n")
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"WATI API request failed for {phone}: {e}")
        return False

    # -------- RESPONSE CHECK --------
    try:
        data = response.json()
    except ValueError:
        print(f"Invalid JSON response from WATI for {phone}")
        return False

    if not data.get("result"):
        print(f"WATI send failed for {phone}: {data}")
        return False

    return True
