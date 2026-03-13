"""Tests for the FastAPI Energy API (110-api/).

Mocks external dependencies (psycopg2, elasticsearch) to allow
unit-testing API endpoints and Pydantic models without a database.
"""

import sys
import types
from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
API_DIR = REPO_ROOT / "110-api"

# ── Bootstrap: make 110-api/ importable as "api" ────────────────────────
# Mirrors the Docker layout (PYTHONPATH=/app, code in /app/api/).

# Mock heavy external deps not needed for unit tests
sys.modules.setdefault("psycopg2", MagicMock())
_es = types.ModuleType("elasticsearch")
_es.Elasticsearch = MagicMock()
sys.modules.setdefault("elasticsearch", _es)

# Register "api" package pointing to 110-api/
_api_pkg = types.ModuleType("api")
_api_pkg.__path__ = [str(API_DIR)]
_api_pkg.__package__ = "api"
sys.modules.setdefault("api", _api_pkg)

# Register "api.services" sub-package (no __init__.py in source)
_svc_pkg = types.ModuleType("api.services")
_svc_pkg.__path__ = [str(API_DIR / "services")]
_svc_pkg.__package__ = "api.services"
sys.modules.setdefault("api.services", _svc_pkg)

from fastapi.testclient import TestClient  # noqa: E402

from api.main import app  # noqa: E402
from api.models import ConsumptionForecast, HourlyPrediction  # noqa: E402

client = TestClient(app)


# ── /health ─────────────────────────────────────────────────────────────

class TestHealth:
    def test_returns_ok(self):
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json() == {"status": "ok"}


# ── /forecast/consumption ───────────────────────────────────────────────

class TestForecastConsumption:
    @patch("api.main.fetch_consumption_forecast")
    def test_returns_forecast(self, mock_fetch):
        mock_fetch.return_value = [
            {"hour": datetime(2024, 1, 1, 0), "predicted": 1.5, "lower": 1.0, "upper": 2.0},
            {"hour": datetime(2024, 1, 1, 1), "predicted": 2.0, "lower": 1.5, "upper": 2.5},
        ]
        r = client.get("/forecast/consumption", params={"date": "2024-01-01"})
        assert r.status_code == 200
        body = r.json()
        assert body["date"] == "2024-01-01"
        assert len(body["predictions"]) == 2

    @patch("api.main.fetch_consumption_forecast")
    def test_returns_404_when_no_data(self, mock_fetch):
        mock_fetch.return_value = None
        r = client.get("/forecast/consumption", params={"date": "2099-01-01"})
        assert r.status_code == 404

    def test_missing_date_returns_422(self):
        r = client.get("/forecast/consumption")
        assert r.status_code == 422


# ── Pydantic models ────────────────────────────────────────────────────

class TestModels:
    def test_hourly_prediction(self):
        hp = HourlyPrediction(
            hour=datetime(2024, 1, 1, 12), predicted=1.5, lower=1.0, upper=2.0
        )
        assert hp.predicted == 1.5

    def test_consumption_forecast(self):
        fc = ConsumptionForecast(
            date=date(2024, 1, 1),
            predictions=[
                HourlyPrediction(hour=datetime(2024, 1, 1, 0), predicted=1.0)
            ],
        )
        assert len(fc.predictions) == 1
