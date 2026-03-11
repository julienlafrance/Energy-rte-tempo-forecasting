#!/usr/bin/env python3
"""
mlops_forecast_linky_705.py - Prévision consommation Linky pour Projet 705

Source : PostgreSQL dbt_gold.linky_hourly
Modèle : SARIMA(2,0,0)(2,1,0,24) — paramètres fixés (optimisés via auto_arima IoT)
Output : PostgreSQL gold.mlops_linky_forecast
Tracking : MLflow
Registry : S3 (optionnel) + MLflow Model Registry (optionnel)

Usage :
  uv run mlops_forecast_linky_705.py
"""

# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "statsmodels",
#     "pandas",
#     "numpy",
#     "psycopg2-binary",
#     "scipy",
#     "mlflow",
#     "boto3",
# ]
# ///

import pandas as pd
import numpy as np
from statsmodels.tsa.statespace.sarimax import SARIMAX
from scipy.stats import ks_2samp
import mlflow
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import warnings
import os
import io
import json
import pickle
import boto3
from urllib import request, error

warnings.filterwarnings("ignore")

# ============================================
# CONFIGURATION
# ============================================

# PostgreSQL (conteneur projet-db)
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_DB = os.environ.get("PG_DB", "airflow")
PG_USER = os.environ["PG_USER"]
PG_PASS = os.environ["PG_PASS"]

# MLflow
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://127.0.0.1:8050")
MLFLOW_EXPERIMENT = "mlops_linky_sarima_705"
MLFLOW_REGISTERED_MODEL_NAME = os.environ.get("MLFLOW_REGISTERED_MODEL_NAME", "linky_sarima_705")

# Notifications
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")

# Model Registry (S3)
MODEL_REGISTRY_S3_BUCKET = os.environ.get("MODEL_REGISTRY_S3_BUCKET", "705")
MODEL_REGISTRY_S3_PREFIX = os.environ.get("MODEL_REGISTRY_S3_PREFIX", "mlops/linky-sarima-705")
MODEL_REGISTRY_S3_ENDPOINT_URL = os.environ.get(
    "MODEL_REGISTRY_S3_ENDPOINT_URL",
    os.environ.get("MLFLOW_S3_ENDPOINT_URL", ""),
)

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
    """Crée la table gold.mlops_linky_forecast si elle n'existe pas."""
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS gold.mlops_linky_forecast (
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


def create_performance_table(conn):
    """Crée la table gold.mlops_linky_performance si elle n'existe pas (rolling par horizon)."""
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS gold.mlops_linky_performance (
            forecast_date TIMESTAMPTZ NOT NULL,
            evaluation_date TIMESTAMPTZ NOT NULL,
            horizon_hours INT NOT NULL,
            mae DOUBLE PRECISION,
            mse DOUBLE PRECISION,
            rmse DOUBLE PRECISION,
            mape DOUBLE PRECISION,
            coverage_80 DOUBLE PRECISION,
            n_points INT,
            model_order TEXT,
            PRIMARY KEY (forecast_date, evaluation_date, horizon_hours)
        );
        """)
        cur.execute("""
        ALTER TABLE gold.mlops_linky_performance
        ADD COLUMN IF NOT EXISTS mse DOUBLE PRECISION;
        """)
    conn.commit()


def create_drift_table(conn):
    """Crée la table gold.mlops_linky_drift si elle n'existe pas."""
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS gold.mlops_linky_drift (
            forecast_date TIMESTAMPTZ NOT NULL PRIMARY KEY,
            ks_statistic DOUBLE PRECISION,
            ks_pvalue DOUBLE PRECISION,
            drift_detected BOOLEAN,
            reference_period TEXT
        );
        """)
    conn.commit()


def save_to_postgres(conn, forecast_df, model_order_str):
    """Sauvegarde les prédictions dans gold.mlops_linky_forecast (une seule prédiction par heure)."""
    now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)

    # Dédupliquer par heure côté DataFrame (garde la dernière occurrence)
    forecast_df_clean = forecast_df[~forecast_df.index.duplicated(keep="last")].sort_index()
    n_removed = len(forecast_df) - len(forecast_df_clean)
    if n_removed > 0:
        print(f"  ⚠ {n_removed} doublon(s) d'heure supprimé(s) avant insertion")

    rows = []
    hours_to_replace = []
    for _, row in forecast_df_clean.iterrows():
        hour_ts = row.name.to_pydatetime()
        hours_to_replace.append(hour_ts)
        rows.append((
            now,
            hour_ts,
            float(row["predicted"]),
            float(row["lower"]),
            float(row["upper"]),
            model_order_str
        ))

    if not rows:
        return now

    with conn.cursor() as cur:
        # Garantit qu'il ne reste qu'une prédiction par heure (tous runs confondus)
        cur.execute(
            "DELETE FROM gold.mlops_linky_forecast WHERE hour = ANY(%s)",
            (hours_to_replace,),
        )
        execute_values(
            cur,
            """INSERT INTO gold.mlops_linky_forecast 
               (forecast_date, hour, conso_kwh_predicted, conso_kwh_lower, conso_kwh_upper, model_order)
               VALUES %s""",
            rows
        )
    conn.commit()
    return now


def evaluate_previous_forecast(conn, horizon_hours=72):
    """
    À chaque exécution, évalue la dernière prévision dont l'horizon complet est observé.
    Concrètement: compare les 72h prédites d'un run à ses 72h réelles correspondantes.
    
    Retourne (forecast_date, evaluation_date, horizon_hours, mae, mse, rmse, mape, coverage_80, n_points, model_order)
    ou None si pas de forecast récent.
    
    Exemple avec horizon_hours=72 :
    - Run à 00h : prédit [01h → 72h]
    - Dès 72h plus tard : on évalue ce run sur son horizon complet (72 points)
    """
    now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    
    # Récupérer le forecast le plus récent dont les 72h sont entièrement observées
    with conn.cursor() as cur:
        cur.execute("""
        SELECT DISTINCT forecast_date
        FROM gold.mlops_linky_forecast
        WHERE forecast_date <= %s - (%s * INTERVAL '1 hour')
        ORDER BY forecast_date DESC
        LIMIT 1
        """, (now, horizon_hours))
        result = cur.fetchone()
    
    if not result:
        return None
    
    forecast_date = result[0]
    window_start = forecast_date
    window_end = forecast_date + timedelta(hours=horizon_hours)
    
    # Récupérer les prédictions de ce run sur tout l'horizon
    pred_df = pd.read_sql(
        """SELECT hour, conso_kwh_predicted, conso_kwh_lower, conso_kwh_upper, model_order 
           FROM gold.mlops_linky_forecast 
           WHERE forecast_date = %s 
             AND hour > %s AND hour <= %s
           ORDER BY hour""",
        conn,
        params=(forecast_date, window_start, window_end),
        parse_dates=["hour"]
    )
    
    if pred_df.empty:
        return None
    
    # Récupérer les vraies valeurs observées sur la même fenêtre
    real_df = pd.read_sql(
        """SELECT hour, SUM(consommation_kwh) as conso_kwh
           FROM dbt_gold.linky_hourly
           WHERE hour > %s AND hour <= %s
           GROUP BY hour
           ORDER BY hour""",
        conn,
        params=(window_start, window_end),
        parse_dates=["hour"]
    )
    
    if real_df.empty:
        return None
    
    # Merger
    merged = pd.merge(pred_df, real_df, on="hour", how="inner")
    if merged.empty:
        return None

    merged = merged.replace([np.inf, -np.inf], np.nan)
    merged_core = merged.dropna(subset=["conso_kwh", "conso_kwh_predicted"])
    if merged_core.empty:
        return None
    
    y_true = merged_core["conso_kwh"].values
    y_pred = merged_core["conso_kwh_predicted"].values
    
    # Calculer les métriques
    errors = y_true - y_pred
    mae = np.mean(np.abs(errors))
    mse = np.mean(errors ** 2)
    rmse = np.sqrt(mse)

    non_zero_mask = y_true > 0
    if np.any(non_zero_mask):
        mape = np.mean(np.abs((y_true[non_zero_mask] - y_pred[non_zero_mask]) / y_true[non_zero_mask])) * 100
    else:
        mape = None

    coverage_base = merged.dropna(subset=["conso_kwh", "conso_kwh_lower", "conso_kwh_upper"])
    if coverage_base.empty:
        coverage = None
    else:
        coverage = np.mean(
            (coverage_base["conso_kwh"] >= coverage_base["conso_kwh_lower"]) &
            (coverage_base["conso_kwh"] <= coverage_base["conso_kwh_upper"])
        ) * 100
    
    model_order = pred_df["model_order"].iloc[0]
    
    return forecast_date, now, horizon_hours, mae, mse, rmse, mape, coverage, len(merged_core), model_order


def compute_data_drift(conn, current_series, history_days=HISTORY_DAYS):
    """
    Détecte le data drift avec le test de Kolmogorov-Smirnov.
    Compare la distribution courante avec celle d'il y a HISTORY_DAYS jours.
    Retourne (ks_statistic, ks_pvalue, drift_detected).
    """
    # Récupérer les données précédentes (il y a HISTORY_DAYS jours)
    query = f"""
    SELECT hour, SUM(consommation_kwh) as conso_kwh
    FROM dbt_gold.linky_hourly
    WHERE consommation_kwh IS NOT NULL
      AND hour >= NOW() - INTERVAL '{history_days * 2} days'
      AND hour < NOW() - INTERVAL '{history_days} days'
    GROUP BY hour
    ORDER BY hour
    """
    prev_df = pd.read_sql(query, conn)
    
    if prev_df.empty or prev_df["conso_kwh"].isna().all():
        return None, None, False
    
    previous_series = prev_df["conso_kwh"].dropna().values
    
    if len(previous_series) == 0:
        return None, None, False
    
    # KS test (test bilatéral)
    ks_stat, ks_pval = ks_2samp(current_series, previous_series)
    
    # Drift détecté si p-value < 0.05
    drift_detected = ks_pval < 0.05
    
    return float(ks_stat), float(ks_pval), drift_detected


def save_performance(conn, forecast_date, evaluation_date, horizon_hours, mae, mse, rmse, mape, coverage, n_points, model_order):
    """Sauvegarde la performance rolling dans gold.mlops_linky_performance."""
    # Convertir les NaN en None (NULL SQL)
    mae = None if (mae is None or np.isnan(mae)) else float(mae)
    mse = None if (mse is None or np.isnan(mse)) else float(mse)
    rmse = None if (rmse is None or np.isnan(rmse)) else float(rmse)
    mape = None if (mape is None or np.isnan(mape)) else float(mape)
    coverage = None if (coverage is None or np.isnan(coverage)) else float(coverage)
    n_points = int(n_points) if n_points is not None else None
    
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM gold.mlops_linky_performance
            WHERE forecast_date = %s AND horizon_hours = %s
            """,
            (forecast_date, horizon_hours),
        )
        cur.execute("""
        INSERT INTO gold.mlops_linky_performance (forecast_date, evaluation_date, horizon_hours, mae, mse, rmse, mape, coverage_80, n_points, model_order)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (forecast_date, evaluation_date, horizon_hours) DO UPDATE SET
            mae = EXCLUDED.mae,
            mse = EXCLUDED.mse,
            rmse = EXCLUDED.rmse,
            mape = EXCLUDED.mape,
            coverage_80 = EXCLUDED.coverage_80,
            n_points = EXCLUDED.n_points,
            model_order = EXCLUDED.model_order
        """, (forecast_date, evaluation_date, horizon_hours, mae, mse, rmse, mape, coverage, n_points, model_order))
    conn.commit()


def save_drift(conn, forecast_date, ks_stat, ks_pval, drift_detected):
    """Sauvegarde le drift dans gold.mlops_linky_drift."""
    # Convertir les types numpy en types Python natifs
    ks_stat = None if (ks_stat is None or np.isnan(ks_stat)) else float(ks_stat)
    ks_pval = None if (ks_pval is None or np.isnan(ks_pval)) else float(ks_pval)
    drift_detected = bool(drift_detected) if drift_detected is not None else False
    
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO gold.mlops_linky_drift (forecast_date, ks_statistic, ks_pvalue, drift_detected, reference_period)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (forecast_date) DO UPDATE SET
            ks_statistic = EXCLUDED.ks_statistic,
            ks_pvalue = EXCLUDED.ks_pvalue,
            drift_detected = EXCLUDED.drift_detected,
            reference_period = EXCLUDED.reference_period
        """, (forecast_date, ks_stat, ks_pval, drift_detected, f"{HISTORY_DAYS}d-glissement"))
    conn.commit()


def send_discord_notification(drift_detected, ks_stat=None, ks_pval=None):
    """Envoie une notification Discord en cas de drift détecté ou si tout va bien."""
    if not DISCORD_WEBHOOK_URL:
        print("  ⚠ DISCORD_WEBHOOK_URL non défini, notification Discord ignorée")
        return

    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    if drift_detected:
        content = (
            "🚨 **Data Drift détecté**\n\n"
            f"- Date: {now_str}\n"
            f"- KS statistic: {ks_stat:.4f}\n"
            f"- KS p-value: {ks_pval:.6g}\n"
            "- Action: vérifier les données entrantes et envisager un retrain."
        )
    else:
        content = (
            "✅ **Data Drift: OK (aucune dérive significative)**\n\n"
            f"- Date: {now_str}\n"
            f"- KS statistic: {ks_stat:.4f}\n"
            f"- KS p-value: {ks_pval:.6g}"
        )

    payload = json.dumps({"content": content}).encode("utf-8")
    req = request.Request(
        DISCORD_WEBHOOK_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=10) as response:
            if response.status in (200, 204):
                print("  ✓ Notification Discord envoyée")
            else:
                print(f"  ⚠ Notification Discord non confirmée (status={response.status})")
    except error.URLError as exc:
        print(f"  ⚠ Échec notification Discord: {exc}")


def register_model_to_s3(model, model_order_str, forecast_date):
    """
    Sauvegarde le modèle entraîné dans un bucket S3 (model registry simple).
    Retourne l'URI S3 du modèle, ou None si non configuré.
    """
    if not MODEL_REGISTRY_S3_BUCKET:
        print("  ⚠ MODEL_REGISTRY_S3_BUCKET non défini, export S3 ignoré")
        return None

    version_id = forecast_date.strftime("%Y%m%d%H")
    base_key = f"{MODEL_REGISTRY_S3_PREFIX}/{version_id}"
    model_key = f"{base_key}/model.pkl"
    metadata_key = f"{base_key}/metadata.json"

    model_buffer = io.BytesIO()
    pickle.dump(model, model_buffer)
    model_buffer.seek(0)

    metadata = {
        "forecast_date": forecast_date.isoformat(),
        "model_order": model_order_str,
        "type": "SARIMAXResults",
        "history_days": HISTORY_DAYS,
        "n_periods": N_PERIODS,
    }

    s3_client = boto3.client(
        "s3",
        endpoint_url=MODEL_REGISTRY_S3_ENDPOINT_URL or None,
    )
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


# ============================================
# MAIN
# ============================================

def main():
    print("=" * 60)
    print("PRÉVISION SARIMA - Consommation Linky (Projet 705)")
    print("=" * 60)

    # --- 1. Connexion PostgreSQL ---
    print("\n[1/7] Connexion PostgreSQL...")
    conn = get_pg_connection()
    print(f"  ✓ Connecté à {PG_DB}@{PG_HOST}")

    # --- 2. Évaluation du dernier run complet (72h) ---
    print("\n[2/7] Évaluation performance (72h prédites vs 72h réelles)...")
    create_performance_table(conn)
    perf_result = evaluate_previous_forecast(conn, horizon_hours=72)
    if perf_result:
        forecast_date, eval_date, horizon, mae, mse, rmse, mape, coverage, n_points, model_order = perf_result
        print(f"  ✓ Données trouvées (forecast_date={forecast_date})")
        print(f"    Évaluation horizon complet : +{horizon}h (eval_date={eval_date})")
        print(f"    MAE:  {mae:.4f} kWh/h")
        print(f"    MSE:  {mse:.4f} (kWh/h)^2")
        print(f"    RMSE: {rmse:.4f} kWh/h")
        print(f"    MAPE: {mape:.2f}%" if mape is not None else "    MAPE: n/a (pas de y_true > 0)")
        print(f"    Coverage intervalle 80%: {coverage:.1f}% ({n_points} points)" if coverage is not None else f"    Coverage intervalle 80%: n/a ({n_points} points)")
        save_performance(conn, forecast_date, eval_date, horizon, mae, mse, rmse, mape, coverage, n_points, model_order)
    else:
        print("  ⚠ Pas encore de données observées pour évaluation")
        perf_result = None

    # --- 3. Récupération des données et Data Drift ---
    print(f"\n[3/7] Récupération des {HISTORY_DAYS} derniers jours...")
    df = fetch_consumption(conn, HISTORY_DAYS)
    print(f"  Points bruts: {len(df)}")
    print(f"  Période: {df.index.min()} → {df.index.max()}")

    df = interpolate_missing_hours(df)
    df = cap_outliers(df)
    series = df["conso_kwh"].values
    print(f"  Points après interpolation: {len(series)}")
    print(f"  Conso moyenne: {series.mean():.4f} kWh/h")
    print(f"  Conso max:     {series.max():.4f} kWh/h")

    # --- 4. Détection Data Drift ---
    print("\n[4/7] Détection Data Drift (Kolmogorov-Smirnov)...")
    create_drift_table(conn)
    now_for_drift = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    ks_stat, ks_pval, drift_detected = compute_data_drift(conn, series, HISTORY_DAYS)
    if ks_stat is not None:
        print(f"  KS Statistique: {ks_stat:.4f}")
        print(f"  KS p-value:    {ks_pval:.6f}")
        print(f"  Drift détecté: {'✗ OUI' if drift_detected else '✓ NON'}")
        save_drift(conn, now_for_drift, ks_stat, ks_pval, drift_detected)
        send_discord_notification(drift_detected=drift_detected, ks_stat=ks_stat, ks_pval=ks_pval)
    else:
        print("  ⚠ Pas assez de données pour drift detection")
        ks_stat, ks_pval, drift_detected = None, None, False

    print("  Profil journalier moyen (kWh/h):")
    hourly_mean = df.groupby(df.index.hour)["conso_kwh"].mean()
    for h in range(24):
        val = hourly_mean.get(h, 0)
        bar = "█" * int(val * 20)
        print(f"    {h:02d}h : {val:.3f} {bar}")

    # --- 5. Entraînement SARIMA ---
    print(f"\n[5/7] Entraînement SARIMA (paramètres fixés)...")
    model = train_sarima(series)
    model_order_str = f"SARIMA{ORDER}x{SEASONAL_ORDER}"
    print(f"  ✓ Modèle: {model_order_str}")
    print(f"  AIC: {model.aic:.2f}")
    print(f"  BIC: {model.bic:.2f}")

    # --- 6. Prédiction 72h ---
    print(f"\n[6/7] Génération de {N_PERIODS}h de prévisions...")
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

    # --- 7. Sauvegarde PostgreSQL ---
    print("\n[7/7] Sauvegarde dans gold.mlops_linky_forecast...")
    create_forecast_table(conn)
    forecast_date = save_to_postgres(conn, forecast_df, model_order_str)
    print(f"  ✓ {len(forecast_df)} lignes insérées (forecast_date={forecast_date})")

    # --- 8. Registry modèle (S3) ---
    print("\n[8/8] Registry modèle...")
    s3_model_uri = register_model_to_s3(model, model_order_str, forecast_date)

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
            "source": "postgresql/dbt_gold.linky_hourly",
            "model_registry_s3_bucket": MODEL_REGISTRY_S3_BUCKET or "",
            "model_registry_s3_prefix": MODEL_REGISTRY_S3_PREFIX,
        })
        
        # Métriques de modèle
        mlflow.log_metrics({
            "aic": model.aic,
            "bic": model.bic,
            "mean_consumption": float(series.mean()),
            "std_consumption": float(series.std()),
            "mean_forecast": float(forecast.mean()),
        })
        
        # Métriques de performance (run précédent, rolling)
        if perf_result:
            forecast_date, eval_date, horizon, mae, mse, rmse, mape, coverage, n_points, model_order = perf_result
            perf_metrics = {
                f"performance_last{horizon}h_mae_kwh": mae,
                f"performance_last{horizon}h_mse_kwh2": mse,
                f"performance_last{horizon}h_rmse_kwh": rmse,
                f"performance_last{horizon}h_mape_pct": mape,
                f"performance_last{horizon}h_coverage_interval80_pct": coverage,
                f"performance_last{horizon}h_n_points": float(n_points),
            }
            perf_metrics = {
                key: float(value)
                for key, value in perf_metrics.items()
                if value is not None and np.isfinite(value)
            }
            if perf_metrics:
                mlflow.log_metrics(perf_metrics)
        
        # Métriques de drift
        if ks_stat is not None:
            mlflow.log_metrics({
                "drift_ks_statistic": ks_stat,
                "drift_ks_pvalue": ks_pval,
            })
            mlflow.log_param("drift_ks_detected", str(drift_detected))

        # Logging modèle dans MLflow + enregistrement optionnel dans Model Registry MLflow
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
    print(f"✓ TERMINÉ — {model_order_str}, {N_PERIODS}h de prévisions générées")
    print("=" * 60)


if __name__ == "__main__":
    main()
