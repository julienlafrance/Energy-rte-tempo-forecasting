#!/usr/bin/env python3
"""
forecast_linky_705.py - Prévision consommation Linky pour Projet 705

Source : PostgreSQL dbt_gold.linky_hourly
Modèle : SARIMA(2,0,0)(2,1,0,24) — paramètres fixés (optimisés via auto_arima IoT)
Output : PostgreSQL gold.linky_forecast + Elasticsearch index linky-forecast
Tracking : MLflow

Usage :
  uv run forecast_linky_705.py
"""

# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "statsmodels",
#     "pandas",
#     "numpy",
#     "psycopg2-binary",
#     "elasticsearch>=8.0.0,<9.0.0",
#     "mlflow",
#     "boto3",
# ]
# ///

import pandas as pd
import numpy as np
from statsmodels.tsa.statespace.sarimax import SARIMAX
import mlflow
import psycopg2
from psycopg2.extras import execute_values
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import warnings
import os

warnings.filterwarnings("ignore")

# ============================================
# CONFIGURATION
# ============================================

# PostgreSQL (conteneur projet-db)
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_DB = os.environ.get("PG_DB", "airflow")
PG_USER = os.environ.get("PG_USER", "airflow")
PG_PASS = os.environ.get("PG_PASS", "airflow")

# Elasticsearch
ES_HOST = os.environ.get("ES_HOST", "http://localhost:9200")
ES_USER = os.environ.get("ES_USER", "elastic")
ES_PASS = os.environ.get("ES_PASS", "")

# MLflow
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://127.0.0.1:8050")
MLFLOW_EXPERIMENT = "linky_sarima_705"

# Paramètres modèle — fixés, déjà optimisés via auto_arima sur le projet IoT
ORDER = (2, 0, 0)
SEASONAL_ORDER = (2, 1, 0, 24)
HISTORY_DAYS = 21
N_PERIODS = 72


# ============================================
# FONCTIONS
# ============================================

def get_pg_connection():
    """Connexion PostgreSQL."""
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASS
    )


def fetch_consumption(conn, days=HISTORY_DAYS):
    """
    Récupère la consommation horaire totale depuis dbt_gold.linky_hourly.
    Agrège tous les tiers par heure.
    """
    query = f"""
    SELECT hour, SUM(consommation_kwh) as conso_kwh
    FROM dbt_gold.linky_hourly
    WHERE consommation_kwh IS NOT NULL
      AND hour >= NOW() - INTERVAL '{days} days'
    GROUP BY hour
    ORDER BY hour
    """
    df = pd.read_sql(query, conn, parse_dates=["hour"])
    df = df.set_index("hour")
    return df


def interpolate_missing_hours(df):
    """
    Resample en fréquence horaire et interpole les heures manquantes.
    """
    full_index = pd.date_range(
        start=df.index.min(),
        end=df.index.max(),
        freq="h"
    )
    df_full = df.reindex(full_index)
    n_missing = df_full["conso_kwh"].isna().sum()
    if n_missing > 0:
        print(f"  ⚠ {n_missing} heures manquantes, interpolation linéaire...")
        df_full["conso_kwh"] = df_full["conso_kwh"].interpolate(method="linear")
    df_full["conso_kwh"] = df_full["conso_kwh"].clip(lower=0)
    return df_full


def cap_outliers(df, col="conso_kwh", factor=3.0):
    """Plafonne les outliers via IQR."""
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
    """
    Entraîne SARIMA(2,0,0)(2,1,0,24) — paramètres fixés.
    ~10-20s au lieu de 5h+ avec auto_arima sur 500 points.
    """
    print(f"  Paramètres: SARIMA{ORDER}x{SEASONAL_ORDER}")
    model = SARIMAX(
        series,
        order=ORDER,
        seasonal_order=SEASONAL_ORDER,
        enforce_stationarity=False,
        enforce_invertibility=False
    )
    fitted = model.fit(disp=True)
    return fitted


def create_forecast_table(conn):
    """Crée la table gold.linky_forecast si elle n'existe pas."""
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS gold.linky_forecast (
            forecast_date TIMESTAMPTZ NOT NULL,
            hour TIMESTAMPTZ NOT NULL,
            conso_kwh_predicted DOUBLE PRECISION NOT NULL,
            conso_kwh_lower DOUBLE PRECISION,
            conso_kwh_upper DOUBLE PRECISION,
            model_order TEXT,
            PRIMARY KEY (forecast_date, hour)
        );
        """)
    conn.commit()


def save_to_postgres(conn, forecast_df, model_order_str):
    """Sauvegarde les prédictions dans gold.linky_forecast."""
    now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    rows = []
    for _, row in forecast_df.iterrows():
        rows.append((
            now,
            row.name.to_pydatetime(),
            float(row["predicted"]),
            float(row["lower"]),
            float(row["upper"]),
            model_order_str
        ))
    with conn.cursor() as cur:
        cur.execute("DELETE FROM gold.linky_forecast WHERE forecast_date = %s", (now,))
        execute_values(
            cur,
            """INSERT INTO gold.linky_forecast 
               (forecast_date, hour, conso_kwh_predicted, conso_kwh_lower, conso_kwh_upper, model_order)
               VALUES %s""",
            rows
        )
    conn.commit()
    return now


def index_to_elasticsearch(forecast_df, forecast_date, model_order_str):
    """Indexe les prédictions dans Elasticsearch 8.x."""
    if not ES_PASS:
        print("  ⚠ ES_PASS non défini, indexation Elasticsearch ignorée")
        return

    es = Elasticsearch(ES_HOST, basic_auth=(ES_USER, ES_PASS), verify_certs=False)

    # Créer l'index si nécessaire (compatible ES 8.x)
    try:
        es.indices.get(index="linky-forecast")
    except Exception:
        es.indices.create(index="linky-forecast", mappings={
            "properties": {
                "forecast_date":      {"type": "date"},
                "hour":               {"type": "date"},
                "conso_kwh_predicted": {"type": "float"},
                "conso_kwh_lower":    {"type": "float"},
                "conso_kwh_upper":    {"type": "float"},
                "model_order":        {"type": "keyword"},
                "horizon_h":          {"type": "integer"}
            }
        })
        print("  ✓ Index linky-forecast créé")

    for i, (ts, row) in enumerate(forecast_df.iterrows()):
        doc = {
            "forecast_date": forecast_date.isoformat(),
            "hour": ts.isoformat(),
            "conso_kwh_predicted": float(row["predicted"]),
            "conso_kwh_lower": float(row["lower"]),
            "conso_kwh_upper": float(row["upper"]),
            "model_order": model_order_str,
            "horizon_h": i + 1
        }
        es.index(
            index="linky-forecast",
            id=f"{forecast_date.strftime('%Y%m%d%H')}-h{i+1:03d}",
            document=doc
        )
    print(f"  ✓ {len(forecast_df)} documents indexés dans linky-forecast")


# ============================================
# MAIN
# ============================================

def main():
    print("=" * 60)
    print("PRÉVISION SARIMA - Consommation Linky (Projet 705)")
    print("=" * 60)

    # --- 1. Connexion PostgreSQL ---
    print("\n[1/6] Connexion PostgreSQL...")
    conn = get_pg_connection()
    print(f"  ✓ Connecté à {PG_DB}@{PG_HOST}")

    # --- 2. Récupération des données ---
    print(f"\n[2/6] Récupération des {HISTORY_DAYS} derniers jours...")
    df = fetch_consumption(conn, HISTORY_DAYS)
    print(f"  Points bruts: {len(df)}")
    print(f"  Période: {df.index.min()} → {df.index.max()}")

    df = interpolate_missing_hours(df)
    df = cap_outliers(df)
    series = df["conso_kwh"].values
    print(f"  Points après interpolation: {len(series)}")
    print(f"  Conso moyenne: {series.mean():.4f} kWh/h")
    print(f"  Conso max:     {series.max():.4f} kWh/h")

    print("  Profil journalier moyen (kWh/h):")
    hourly_mean = df.groupby(df.index.hour)["conso_kwh"].mean()
    for h in range(24):
        val = hourly_mean.get(h, 0)
        bar = "█" * int(val * 20)
        print(f"    {h:02d}h : {val:.3f} {bar}")

    # --- 3. Entraînement SARIMA ---
    print(f"\n[3/6] Entraînement SARIMA (paramètres fixés)...")
    model = train_sarima(series)
    model_order_str = f"SARIMA{ORDER}x{SEASONAL_ORDER}"
    print(f"  ✓ Modèle: {model_order_str}")
    print(f"  AIC: {model.aic:.2f}")
    print(f"  BIC: {model.bic:.2f}")

    # --- 4. Prédiction 72h ---
    print(f"\n[4/6] Génération de {N_PERIODS}h de prévisions...")
    forecast_result = model.get_forecast(steps=N_PERIODS)
    forecast = np.asarray(forecast_result.predicted_mean)
    conf_int = np.asarray(forecast_result.conf_int(alpha=0.20))

    # Clamp négatifs
    forecast = np.maximum(forecast, 0)
    conf_int[:, 0] = np.maximum(conf_int[:, 0], 0)

    last_ts = df.index.max()
    forecast_index = pd.date_range(
        start=last_ts + timedelta(hours=1),
        periods=N_PERIODS,
        freq="h"
    )
    forecast_df = pd.DataFrame({
        "predicted": forecast,
        "lower": conf_int[:, 0],
        "upper": conf_int[:, 1]
    }, index=forecast_index)

    print(f"  Période prédite: {forecast_index[0]} → {forecast_index[-1]}")
    print(f"  Conso prédite moyenne: {forecast.mean():.4f} kWh/h")

    # --- 5. Sauvegarde PostgreSQL ---
    print("\n[5/6] Sauvegarde dans gold.linky_forecast...")
    create_forecast_table(conn)
    forecast_date = save_to_postgres(conn, forecast_df, model_order_str)
    print(f"  ✓ {len(forecast_df)} lignes insérées (forecast_date={forecast_date})")

    # --- 6. Indexation Elasticsearch ---
    print("\n[6/6] Indexation Elasticsearch...")
    index_to_elasticsearch(forecast_df, forecast_date, model_order_str)

    # --- MLflow tracking ---
    print("\n[+] Logging MLflow...")
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)
    with mlflow.start_run():
        mlflow.log_params({
            "order": str(ORDER),
            "seasonal_order": str(SEASONAL_ORDER),
            "history_days": HISTORY_DAYS,
            "n_periods": N_PERIODS,
            "n_points": len(series),
            "source": "postgresql/dbt_gold.linky_hourly"
        })
        mlflow.log_metrics({
            "aic": model.aic,
            "bic": model.bic,
            "mean_consumption": float(series.mean()),
            "std_consumption": float(series.std()),
            "mean_forecast": float(forecast.mean()),
        })
    print("  ✓ Run MLflow enregistré")

    conn.close()
    print("\n" + "=" * 60)
    print(f"✓ TERMINÉ — {model_order_str}, {N_PERIODS}h de prévisions générées")
    print("=" * 60)


if __name__ == "__main__":
    main()
