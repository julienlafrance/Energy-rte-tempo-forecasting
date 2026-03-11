#!/usr/bin/env python3
"""
mlops_train_linky_705.py - Entraînement SARIMA consommation Linky pour Projet 705

Source : PostgreSQL dbt_gold.linky_hourly
Modèle : SARIMA(2,0,0)(2,1,0,24)
Output : MLflow (artifact model) + MLflow Registry (optionnel) + S3 (optionnel)

Usage :
  uv run mlops_train_linky_705.py
"""

# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "statsmodels",
#     "pandas",
#     "numpy",
#     "psycopg2-binary",
#     "mlflow",
#     "boto3",
# ]
# ///

import io
import json
import os
import pickle
import warnings
from datetime import datetime

import boto3
import mlflow
import numpy as np
import pandas as pd
import psycopg2
from statsmodels.tsa.statespace.sarimax import SARIMAX

warnings.filterwarnings("ignore")


# ============================================
# CONFIGURATION
# ============================================

PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_DB = os.environ.get("PG_DB", "airflow")
PG_USER = os.environ["PG_USER"]
PG_PASS = os.environ["PG_PASS"]

MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://127.0.0.1:8050")
MLFLOW_EXPERIMENT = "mlops_linky_sarima_705"
MLFLOW_REGISTERED_MODEL_NAME = os.environ.get("MLFLOW_REGISTERED_MODEL_NAME", "linky_sarima_705")

MODEL_REGISTRY_S3_BUCKET = os.environ.get("MODEL_REGISTRY_S3_BUCKET", "705")
MODEL_REGISTRY_S3_PREFIX = os.environ.get("MODEL_REGISTRY_S3_PREFIX", "mlops/linky-sarima-705")
MODEL_REGISTRY_S3_ENDPOINT_URL = os.environ.get(
    "MODEL_REGISTRY_S3_ENDPOINT_URL",
    os.environ.get("MLFLOW_S3_ENDPOINT_URL", ""),
)

ORDER = (2, 0, 0)
SEASONAL_ORDER = (2, 1, 0, 24)
HISTORY_DAYS = 21
N_PERIODS = 72


def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
    )


def fetch_consumption(conn, days=HISTORY_DAYS):
    query = f"""
    SELECT hour, SUM(consommation_kwh) as conso_kwh
    FROM dbt_gold.linky_hourly
    WHERE consommation_kwh IS NOT NULL
      AND hour >= NOW() - INTERVAL '{days} days'
    GROUP BY hour
    ORDER BY hour
    """
    df = pd.read_sql(query, conn, parse_dates=["hour"])
    return df.set_index("hour")


def interpolate_missing_hours(df):
    full_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq="h")
    df_full = df.reindex(full_index)
    n_missing = df_full["conso_kwh"].isna().sum()
    if n_missing > 0:
        print(f"  ⚠ {n_missing} heures manquantes, interpolation linéaire...")
        df_full["conso_kwh"] = df_full["conso_kwh"].interpolate(method="linear")
    df_full["conso_kwh"] = df_full["conso_kwh"].clip(lower=0)
    return df_full


def cap_outliers(df, col="conso_kwh", factor=3.0):
    q1 = df[col].quantile(0.25)
    q3 = df[col].quantile(0.75)
    iqr = q3 - q1
    upper_cap = q3 + factor * iqr
    n_capped = (df[col] > upper_cap).sum()
    if n_capped > 0:
        print(f"  ⚠ {n_capped} outliers plafonnés à {upper_cap:.3f} kWh/h")
        df[col] = df[col].clip(upper=upper_cap)
    return df


def train_sarima(series):
    print(f"  Paramètres: SARIMA{ORDER}x{SEASONAL_ORDER}")
    model = SARIMAX(
        series,
        order=ORDER,
        seasonal_order=SEASONAL_ORDER,
        enforce_stationarity=False,
        enforce_invertibility=False,
    )
    return model.fit(disp=True)


def register_model_to_s3(model, model_order_str, train_date):
    if not MODEL_REGISTRY_S3_BUCKET:
        print("  ⚠ MODEL_REGISTRY_S3_BUCKET non défini, export S3 ignoré")
        return None

    version_id = train_date.strftime("%Y%m%d%H")
    base_key = f"{MODEL_REGISTRY_S3_PREFIX}/{version_id}"
    model_key = f"{base_key}/model.pkl"
    metadata_key = f"{base_key}/metadata.json"

    model_buffer = io.BytesIO()
    pickle.dump(model, model_buffer)
    model_buffer.seek(0)

    metadata = {
        "train_date": train_date.isoformat(),
        "model_order": model_order_str,
        "type": "SARIMAXResults",
        "history_days": HISTORY_DAYS,
        "n_periods": N_PERIODS,
    }

    s3_client = boto3.client("s3", endpoint_url=MODEL_REGISTRY_S3_ENDPOINT_URL or None)
    s3_client.upload_fileobj(model_buffer, MODEL_REGISTRY_S3_BUCKET, model_key)
    s3_client.put_object(
        Bucket=MODEL_REGISTRY_S3_BUCKET,
        Key=metadata_key,
        Body=json.dumps(metadata, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    model_uri = f"s3://{MODEL_REGISTRY_S3_BUCKET}/{model_key}"
    print(f"  ✓ Modèle enregistré dans S3: {model_uri}")
    return model_uri


def main():
    print("=" * 60)
    print("ENTRAÎNEMENT SARIMA - Consommation Linky (Projet 705)")
    print("=" * 60)

    print("\n[1/4] Connexion PostgreSQL...")
    conn = get_pg_connection()
    print(f"  ✓ Connecté à {PG_DB}@{PG_HOST}")

    print(f"\n[2/4] Récupération des {HISTORY_DAYS} derniers jours...")
    df = fetch_consumption(conn, HISTORY_DAYS)
    df = interpolate_missing_hours(df)
    df = cap_outliers(df)
    series = df["conso_kwh"].values
    print(f"  ✓ Points utilisés: {len(series)}")

    print("\n[3/4] Entraînement modèle...")
    model = train_sarima(series)
    model_order_str = f"SARIMA{ORDER}x{SEASONAL_ORDER}"
    train_date = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    print(f"  ✓ Modèle: {model_order_str}")
    print(f"  AIC: {model.aic:.2f}")
    print(f"  BIC: {model.bic:.2f}")

    print("\n[4/4] Tracking MLflow + registry...")
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    s3_model_uri = register_model_to_s3(model, model_order_str, train_date)

    with mlflow.start_run():
        mlflow.set_tag("phase", "train")
        mlflow.log_params(
            {
                "order": str(ORDER),
                "seasonal_order": str(SEASONAL_ORDER),
                "history_days": HISTORY_DAYS,
                "n_periods": N_PERIODS,
                "n_points": len(series),
                "source": "postgresql/dbt_gold.linky_hourly",
                "model_registry_s3_bucket": MODEL_REGISTRY_S3_BUCKET or "",
                "model_registry_s3_prefix": MODEL_REGISTRY_S3_PREFIX,
            }
        )
        mlflow.log_metrics(
            {
                "aic": model.aic,
                "bic": model.bic,
                "mean_consumption": float(np.mean(series)),
                "std_consumption": float(np.std(series)),
            }
        )

        mlflow.statsmodels.log_model(model, artifact_path="model")
        run = mlflow.active_run()
        if run:
            run_model_uri = f"runs:/{run.info.run_id}/model"
            mlflow.log_param("mlflow_model_uri", run_model_uri)

            if MLFLOW_REGISTERED_MODEL_NAME:
                try:
                    registered = mlflow.register_model(
                        model_uri=run_model_uri,
                        name=MLFLOW_REGISTERED_MODEL_NAME,
                    )
                    mlflow.log_param("mlflow_registered_model_name", MLFLOW_REGISTERED_MODEL_NAME)
                    mlflow.log_param("mlflow_registered_model_version", str(registered.version))
                    print(f"  ✓ Modèle enregistré dans MLflow Registry: {MLFLOW_REGISTERED_MODEL_NAME} v{registered.version}")
                except Exception as exc:
                    print(f"  ⚠ Enregistrement MLflow Registry échoué: {exc}")

        if s3_model_uri:
            mlflow.log_param("s3_model_uri", s3_model_uri)

    print("  ✓ Run MLflow enregistré")
    conn.close()

    print("\n" + "=" * 60)
    print(f"✓ TERMINÉ — modèle entraîné et versionné: {model_order_str}")
    print("=" * 60)


if __name__ == "__main__":
    main()
