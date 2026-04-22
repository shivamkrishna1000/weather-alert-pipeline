RULES = {
    "rain": [
        {
            "id": "R1",
            "condition": lambda w: w["max_rain"] >= 20 or w["rain_hours"] >= 4,
            "message": "Heavy rain expected. Ensure proper drainage and avoid irrigation.",
        },
        {
            "id": "R2",
            "condition": lambda w: w["max_rain"] >= 5 or w["rain_probability"] >= 70,
            "message": "Rain likely. Avoid irrigation today.",
        },
        {
            "id": "R3",
            "condition": lambda w: w["rain_probability"] >= 50,
            "message": "Light rain possible. Plan irrigation carefully.",
        },
    ],
    "wind": [
        {
            "id": "W1",
            "condition": lambda w: w["max_wind"] >= 20,
            "message": "Strong winds expected. Avoid pesticide spraying.",
        },
        {
            "id": "W2",
            "condition": lambda w: w["max_wind"] >= 15,
            "message": "Moderate wind. Be cautious with spraying.",
        },
    ],
    "humidity": [
        {
            "id": "H1",
            "condition": lambda w: w["max_humidity"] >= 85,
            "message": "High humidity may cause crop diseases. Monitor crops closely.",
        },
        {
            "id": "H2",
            "condition": lambda w: w["max_humidity"] >= 78,
            "message": "Humidity is elevated. Watch for fungal infections.",
        },
    ],
    "temperature": [
        {
            "id": "T1",
            "condition": lambda w: w["max_temp"] >= 38,
            "message": "High temperature may stress crops. Ensure proper irrigation.",
        },
        {
            "id": "T2",
            "condition": lambda w: w["max_temp"] >= 32,
            "message": "Warm conditions. Monitor soil moisture.",
        },
        {
            "id": "T3",
            "condition": lambda w: w["min_temp"] <= 13,
            "message": "Low temperature may affect crops. Take protective measures.",
        },
    ],
}
