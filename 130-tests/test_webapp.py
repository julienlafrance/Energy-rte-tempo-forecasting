"""Tests for the Streamlit webapp (120-webapp/).

Tests configuration and the API client without needing a running
Streamlit server or the actual backend API.
"""

import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests as _requests

REPO_ROOT = Path(__file__).resolve().parents[1]
WEBAPP_DIR = REPO_ROOT / "120-webapp"

# Add 120-webapp/ to sys.path so bare imports work
# (mirrors the Docker WORKDIR /app layout)
if str(WEBAPP_DIR) not in sys.path:
    sys.path.insert(0, str(WEBAPP_DIR))


# ── config.py ───────────────────────────────────────────────────────────

class TestConfig:
    def test_default_api_url(self, monkeypatch):
        monkeypatch.delenv("API_URL", raising=False)
        import importlib

        import config
        importlib.reload(config)
        assert config.API_URL == "http://localhost:8000"

    def test_api_url_from_env(self, monkeypatch):
        monkeypatch.setenv("API_URL", "http://custom:9000")
        import importlib

        import config
        importlib.reload(config)
        assert config.API_URL == "http://custom:9000"


# ── api_client.py ───────────────────────────────────────────────────────

class TestApiClient:
    @patch("api_client.requests.get")
    def test_check_health_ok(self, mock_get):
        mock_get.return_value = MagicMock(status_code=200)
        mock_get.return_value.json.return_value = {"status": "ok"}
        mock_get.return_value.raise_for_status = MagicMock()
        from api_client import check_health

        result = check_health()
        assert result == {"status": "ok"}

    @patch("api_client.requests.get")
    def test_check_health_failure(self, mock_get):
        mock_get.side_effect = _requests.RequestException("Connection error")
        from api_client import check_health

        result = check_health()
        assert result is None

    @patch("api_client.requests.get")
    def test_get_consumption_forecast_ok(self, mock_get):
        mock_get.return_value = MagicMock(status_code=200)
        mock_get.return_value.json.return_value = {
            "date": "2024-01-01",
            "predictions": [],
        }
        mock_get.return_value.raise_for_status = MagicMock()
        from api_client import get_consumption_forecast

        result = get_consumption_forecast(date(2024, 1, 1))
        assert result is not None
        assert result["date"] == "2024-01-01"

    @patch("api_client.requests.get")
    def test_get_consumption_forecast_404(self, mock_get):
        mock_get.return_value = MagicMock(status_code=404)
        from api_client import get_consumption_forecast

        result = get_consumption_forecast(date(2099, 1, 1))
        assert result is None

    @patch("api_client.requests.get")
    def test_get_consumption_forecast_error(self, mock_get):
        mock_get.side_effect = _requests.RequestException("Timeout")
        from api_client import get_consumption_forecast

        result = get_consumption_forecast(date(2024, 1, 1))
        assert result is None
