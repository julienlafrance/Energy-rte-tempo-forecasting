import requests
from datetime import date
from config import API_URL


def check_health() -> dict | None:
    """Appelle GET /health et retourne la réponse JSON."""
    try:
        r = requests.get(f"{API_URL}/health", timeout=5)
        r.raise_for_status()
        return r.json()
    except requests.RequestException:
        return None


def get_consumption_forecast(target_date: date) -> dict | None:
    """Appelle GET /forecast/consumption?date=YYYY-MM-DD."""
    try:
        r = requests.get(
            f"{API_URL}/forecast/consumption",
            params={"date": target_date.isoformat()},
            timeout=10,
        )
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except requests.RequestException:
        return None
