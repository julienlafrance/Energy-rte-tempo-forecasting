import os
import time
import logging
from datetime import date as _date, datetime, timezone

from fastapi import FastAPI, HTTPException, Query, Request
from elasticsearch import Elasticsearch

from api.models import ConsumptionForecast, HourlyPrediction
from api.services.consumption_service import fetch_consumption_forecast

logger = logging.getLogger("api")

app = FastAPI(title="MLOps Energy API", version="1.0.0")

# --- Elasticsearch client ---
ES_URL = os.environ.get("ES_URL", "http://elasticsearch:9200")
ES_USER = os.environ.get("ES_USER", "elastic")
ES_PASS = os.environ.get("ES_PASS", "")
ES_INDEX = "api-logs"

es = Elasticsearch(ES_URL, basic_auth=(ES_USER, ES_PASS)) if ES_PASS else None


@app.middleware("http")
async def log_to_elasticsearch(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    duration_ms = round((time.time() - start) * 1000, 2)

    if es:
        doc = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "method": request.method,
            "path": request.url.path,
            "query_params": str(request.query_params),
            "status_code": response.status_code,
            "duration_ms": duration_ms,
            "client_ip": request.client.host if request.client else None,
        }
        try:
            es.index(index=ES_INDEX, document=doc)
        except Exception as e:
            logger.warning("ES indexing failed: %s", e)

    return response


@app.get(
    "/health",
    summary="Health check",
    description="Vérifie que l'API est opérationnelle.",
    responses={200: {"description": "API opérationnelle"}}
)
def health():
    return {"status": "ok"}

@app.get("/forecast/consumption")
def forecast_consumption(date: _date):
    """
    Endpoint de forecast de consommation.
    Les inputs ne sont pas encore définis.
    """

    rows = fetch_consumption_forecast(date)
    if rows is None:
        raise HTTPException(status_code=404, detail="Prévision non disponible pour cette date")

    hourly_predictions = [
        HourlyPrediction(
            hour=r["hour"],
            predicted=r["predicted"],
            lower=r["lower"],
            upper=r["upper"]
        ) for r in rows
    ]

    return ConsumptionForecast(
        date=date,
        predictions=hourly_predictions
    )