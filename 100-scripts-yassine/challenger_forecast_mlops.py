#!/usr/bin/env python3
"""
challenger_forecast_mlops.py - Challenger SARIMA pour Projet 705

Source : PostgreSQL dbt_gold.linky_hourly
Modèle : SARIMA(2,0,0)(2,1,0,24) — paramètres fixés (optimisés via auto_arima IoT)
Output : PostgreSQL gold.mlops_linky_forecast
Tracking : MLflow
Registry : S3 (optionnel) + MLflow Model Registry (optionnel)

Usage :
    uv run challenger_forecast_mlops.py
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
MLFLOW_EXPERIMENT = os.environ.get("MLFLOW_EXPERIMENT", "mlops_linky_sarima_705_challenger")
MLFLOW_REGISTERED_MODEL_NAME = os.environ.get("MLFLOW_REGISTERED_MODEL_NAME", "linky_sarima_705_challenger")

# Notifications
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")

# Challenger / Champion
MODEL_NAME = os.environ.get("MODEL_NAME", "challenger")
CHAMPION_MODEL_NAME = os.environ.get("CHAMPION_MODEL_NAME", "champion")
CHAMPION_PERF_TABLE = os.environ.get("CHAMPION_PERF_TABLE", "gold.mlops_linky_performance")
CHALLENGER_FORECAST_TABLE = os.environ.get("CHALLENGER_FORECAST_TABLE", "gold.mlops_linky_forecast_challenger")
CHALLENGER_PERF_TABLE = os.environ.get("CHALLENGER_PERF_TABLE", "gold.mlops_linky_performance_challenger")
CHALLENGER_DRIFT_TABLE = os.environ.get("CHALLENGER_DRIFT_TABLE", "gold.mlops_linky_drift_challenger")
CHALLENGER_COMPARE_TABLE = os.environ.get("CHALLENGER_COMPARE_TABLE", "gold.mlops_linky_model_compare")
CHALLENGER_TRIGGER_TABLE = os.environ.get("CHALLENGER_TRIGGER_TABLE", "gold.mlops_linky_challenger_trigger_log")

# Triggering (go/no-go du run challenger)
ENABLE_TRIGGER_GATING = os.environ.get("ENABLE_TRIGGER_GATING", "true").lower() in ("1", "true", "yes", "y")
TRIGGER_MIN_NEW_HOURS = int(os.environ.get("TRIGGER_MIN_NEW_HOURS", "6"))
TRIGGER_WEEKLY_DAY = int(os.environ.get("TRIGGER_WEEKLY_DAY", "0"))
TRIGGER_WEEKLY_HOUR = int(os.environ.get("TRIGGER_WEEKLY_HOUR", "3"))
TRIGGER_PERF_DROP_PCT = float(os.environ.get("TRIGGER_PERF_DROP_PCT", "10"))

# Model Registry (S3)
MODEL_REGISTRY_S3_BUCKET = os.environ.get("MODEL_REGISTRY_S3_BUCKET", "705")
MODEL_REGISTRY_S3_PREFIX = os.environ.get("MODEL_REGISTRY_S3_PREFIX", "mlops/linky-sarima-705/challenger")
MODEL_REGISTRY_S3_ENDPOINT_URL = os.environ.get(
    "MODEL_REGISTRY_S3_ENDPOINT_URL",
    os.environ.get("MLFLOW_S3_ENDPOINT_URL", ""),
)

# Paramètres modèle challenger — configurables pour expérimentation
def _parse_tuple_env(name, default_tuple):
    raw = os.environ.get(name)
    if not raw:
        return default_tuple
    cleaned = raw.strip().replace("(", "").replace(")", "")
    parts = [p.strip() for p in cleaned.split(",") if p.strip() != ""]
    if len(parts) != len(default_tuple):
        raise ValueError(f"{name} doit contenir {len(default_tuple)} entiers, ex: {default_tuple}")
    return tuple(int(p) for p in parts)


ORDER = _parse_tuple_env("CHALLENGER_ORDER", (2, 0, 0))
SEASONAL_ORDER = _parse_tuple_env("CHALLENGER_SEASONAL_ORDER", (2, 1, 0, 24))
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


def save_model_to_local_and_s3(model, model_order_str, forecast_date, conn):
    """
    Sauvegarde le modèle entraîné en local (temp) et en S3.
    Enregistre aussi le chemin dans la DB pour traçabilité.
    Retourne (s3_uri, local_path).
    """
    # Sauvegarde local (temp)
    local_model_dir = "/tmp/challenger_models"
    os.makedirs(local_model_dir, exist_ok=True)
    version_id = forecast_date.strftime("%Y%m%d%H")
    local_model_path = os.path.join(local_model_dir, f"model_{version_id}.pkl")
    
    with open(local_model_path, "wb") as f:
        pickle.dump(model, f)
    
    print(f"  ✓ Modèle sauvegardé localement: {local_model_path}")
    
    # Sauvegarde S3
    s3_uri = register_model_to_s3(model, model_order_str, forecast_date)
    
    # Traçabilité en DB (optionnel, pour audit)
    try:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS gold.mlops_linky_model_versions (
                version_id TEXT PRIMARY KEY,
                model_name TEXT,
                s3_uri TEXT,
                local_path TEXT,
                model_order TEXT,
                trained_at TIMESTAMPTZ
            );
            """)
            cur.execute("""
            INSERT INTO gold.mlops_linky_model_versions
                (version_id, model_name, s3_uri, local_path, model_order, trained_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (version_id) DO UPDATE SET
                s3_uri = EXCLUDED.s3_uri,
                local_path = EXCLUDED.local_path
            """, (version_id, MODEL_NAME, s3_uri, local_model_path, model_order_str, forecast_date))
        conn.commit()
    except Exception as exc:
        print(f"  ⚠ Enregistrement modèle version en DB échoué: {exc}")
    
    return s3_uri, local_model_path


def load_latest_model():
    """
    Charge le dernier modèle entraîné depuis S3 ou local.
    Retourne le modèle ou None si indisponible.
    """
    # D'abord, essayer de charger depuis local (plus rapide)
    local_model_dir = "/tmp/challenger_models"
    if os.path.exists(local_model_dir):
        files = sorted([f for f in os.listdir(local_model_dir) if f.startswith("model_") and f.endswith(".pkl")])
        if files:
            latest_file = files[-1]  # Dernier en ordre alphabétique = plus récent (YYYYMMDDHH)
            local_path = os.path.join(local_model_dir, latest_file)
            try:
                with open(local_path, "rb") as f:
                    model = pickle.load(f)
                print(f"  ✓ Modèle chargé depuis local: {local_path}")
                return model
            except Exception as exc:
                print(f"  ⚠ Échec chargement modèle local {local_path}: {exc}")
    
    # Si pas de local ou échec, essayer S3 (optionnel, peut être coûteux)
    if MODEL_REGISTRY_S3_BUCKET and MODEL_REGISTRY_S3_PREFIX:
        try:
            s3_client = boto3.client("s3", endpoint_url=MODEL_REGISTRY_S3_ENDPOINT_URL or None)
            # Lister les modèles en S3 et charger le plus récent
            # (implémentation simplifée : on suppose une structure prévisible)
            # En pratique, il faudrait une logique plus robuste
            print("  ⚠ Chargement depuis S3 non encore implémenté (trop coûteux), utiliser local")
        except Exception as exc:
            print(f"  ⚠ Échec accès S3: {exc}")
    
    return None


def create_forecast_table(conn):
    """Crée la table forecast du challenger si elle n'existe pas."""
    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHALLENGER_FORECAST_TABLE} (
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
    """Crée la table performance du challenger si elle n'existe pas (rolling par horizon)."""
    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHALLENGER_PERF_TABLE} (
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
        cur.execute(f"""
        ALTER TABLE {CHALLENGER_PERF_TABLE}
        ADD COLUMN IF NOT EXISTS mse DOUBLE PRECISION;
        """)
    conn.commit()


def create_drift_table(conn):
    """Crée la table drift du challenger si elle n'existe pas."""
    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHALLENGER_DRIFT_TABLE} (
            forecast_date TIMESTAMPTZ NOT NULL PRIMARY KEY,
            ks_statistic DOUBLE PRECISION,
            ks_pvalue DOUBLE PRECISION,
            drift_detected BOOLEAN,
            reference_period TEXT
        );
        """)
    conn.commit()


def create_model_compare_table(conn):
    """Crée la table de comparaison champion/challenger si elle n'existe pas."""
    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHALLENGER_COMPARE_TABLE} (
            evaluation_date TIMESTAMPTZ NOT NULL,
            horizon_hours INT NOT NULL,
            champion_mae DOUBLE PRECISION,
            challenger_mae DOUBLE PRECISION,
            mae_delta DOUBLE PRECISION,
            winner TEXT,
            PRIMARY KEY (evaluation_date, horizon_hours)
        );
        """)
    conn.commit()


def create_trigger_table(conn):
    """Crée la table d'audit des triggers challenger."""
    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHALLENGER_TRIGGER_TABLE} (
            run_ts TIMESTAMPTZ NOT NULL PRIMARY KEY,
            model_name TEXT,
            trigger_new_data BOOLEAN,
            trigger_weekly_retrain BOOLEAN,
            trigger_perf_drop BOOLEAN,
            should_run BOOLEAN,
            details JSONB
        );
        """)
    conn.commit()


def evaluate_run_triggers(conn):
    """Évalue les triggers: nouvelles données, hebdo, baisse de perf."""
    now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)

    latest_data_hour_df = pd.read_sql(
        """
        SELECT MAX(hour) AS max_hour
        FROM dbt_gold.linky_hourly
        """,
        conn,
        parse_dates=["max_hour"],
    )
    latest_forecast_hour_df = pd.read_sql(
        f"""
        SELECT MAX(hour) AS max_hour
        FROM {CHALLENGER_FORECAST_TABLE}
        """,
        conn,
        parse_dates=["max_hour"],
    )

    data_max = latest_data_hour_df.iloc[0]["max_hour"] if not latest_data_hour_df.empty else None
    fc_max = latest_forecast_hour_df.iloc[0]["max_hour"] if not latest_forecast_hour_df.empty else None

    if pd.isna(data_max):
        trigger_new_data = False
        new_hours = 0
    elif pd.isna(fc_max):
        trigger_new_data = True
        new_hours = 999999
    else:
        new_hours = int((data_max.to_pydatetime() - fc_max.to_pydatetime()).total_seconds() // 3600)
        trigger_new_data = new_hours >= TRIGGER_MIN_NEW_HOURS

    trigger_weekly_retrain = (now.weekday() == TRIGGER_WEEKLY_DAY and now.hour == TRIGGER_WEEKLY_HOUR)

    perf_df = pd.read_sql(
        f"""
        SELECT evaluation_date, mae
        FROM {CHALLENGER_PERF_TABLE}
        WHERE horizon_hours = %s AND mae IS NOT NULL
        ORDER BY evaluation_date DESC
        LIMIT 2
        """,
        conn,
        params=(N_PERIODS,),
        parse_dates=["evaluation_date"],
    )

    trigger_perf_drop = False
    perf_drop_pct = None
    if len(perf_df) >= 2:
        current_mae = float(perf_df.iloc[0]["mae"])
        previous_mae = float(perf_df.iloc[1]["mae"])
        if previous_mae > 0:
            perf_drop_pct = ((current_mae - previous_mae) / previous_mae) * 100.0
            trigger_perf_drop = perf_drop_pct >= TRIGGER_PERF_DROP_PCT

    should_run = trigger_new_data or trigger_weekly_retrain or trigger_perf_drop

    details = {
        "utc_now": now.isoformat(),
        "latest_data_hour": None if pd.isna(data_max) else data_max.isoformat(),
        "latest_challenger_forecast_hour": None if pd.isna(fc_max) else fc_max.isoformat(),
        "new_hours": int(new_hours),
        "threshold_new_hours": TRIGGER_MIN_NEW_HOURS,
        "weekly_day": TRIGGER_WEEKLY_DAY,
        "weekly_hour": TRIGGER_WEEKLY_HOUR,
        "perf_drop_pct": perf_drop_pct,
        "threshold_perf_drop_pct": TRIGGER_PERF_DROP_PCT,
    }

    return now, {
        "trigger_new_data": bool(trigger_new_data),
        "trigger_weekly_retrain": bool(trigger_weekly_retrain),
        "trigger_perf_drop": bool(trigger_perf_drop),
        "should_run": bool(should_run),
        "details": details,
    }


def save_trigger_log(conn, run_ts, trigger_result):
    """Persiste la décision de trigger pour audit."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {CHALLENGER_TRIGGER_TABLE}
                (run_ts, model_name, trigger_new_data, trigger_weekly_retrain, trigger_perf_drop, should_run, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (run_ts) DO UPDATE SET
                model_name = EXCLUDED.model_name,
                trigger_new_data = EXCLUDED.trigger_new_data,
                trigger_weekly_retrain = EXCLUDED.trigger_weekly_retrain,
                trigger_perf_drop = EXCLUDED.trigger_perf_drop,
                should_run = EXCLUDED.should_run,
                details = EXCLUDED.details
            """,
            (
                run_ts,
                MODEL_NAME,
                bool(trigger_result["trigger_new_data"]),
                bool(trigger_result["trigger_weekly_retrain"]),
                bool(trigger_result["trigger_perf_drop"]),
                bool(trigger_result["should_run"]),
                json.dumps(trigger_result["details"], ensure_ascii=False),
            ),
        )
    conn.commit()


def save_to_postgres(conn, forecast_df, model_order_str):
    """Sauvegarde les prédictions du challenger (une seule prédiction par heure)."""
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
        cur.execute(f"DELETE FROM {CHALLENGER_FORECAST_TABLE} WHERE hour = ANY(%s)", (hours_to_replace,))
        execute_values(
            cur,
            f"""INSERT INTO {CHALLENGER_FORECAST_TABLE}
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
        cur.execute(f"""
        SELECT DISTINCT forecast_date
        FROM {CHALLENGER_FORECAST_TABLE}
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
                f"""SELECT hour, conso_kwh_predicted, conso_kwh_lower, conso_kwh_upper, model_order 
                     FROM {CHALLENGER_FORECAST_TABLE}
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
    """Sauvegarde la performance rolling du challenger."""
    # Convertir les NaN en None (NULL SQL)
    mae = None if (mae is None or np.isnan(mae)) else float(mae)
    mse = None if (mse is None or np.isnan(mse)) else float(mse)
    rmse = None if (rmse is None or np.isnan(rmse)) else float(rmse)
    mape = None if (mape is None or np.isnan(mape)) else float(mape)
    coverage = None if (coverage is None or np.isnan(coverage)) else float(coverage)
    n_points = int(n_points) if n_points is not None else None
    
    with conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {CHALLENGER_PERF_TABLE} WHERE forecast_date = %s AND horizon_hours = %s",
            (forecast_date, horizon_hours),
        )
        cur.execute(f"""
        INSERT INTO {CHALLENGER_PERF_TABLE} (forecast_date, evaluation_date, horizon_hours, mae, mse, rmse, mape, coverage_80, n_points, model_order)
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
    """Sauvegarde le drift du challenger."""
    # Convertir les types numpy en types Python natifs
    ks_stat = None if (ks_stat is None or np.isnan(ks_stat)) else float(ks_stat)
    ks_pval = None if (ks_pval is None or np.isnan(ks_pval)) else float(ks_pval)
    drift_detected = bool(drift_detected) if drift_detected is not None else False
    
    with conn.cursor() as cur:
        cur.execute(f"""
        INSERT INTO {CHALLENGER_DRIFT_TABLE} (forecast_date, ks_statistic, ks_pvalue, drift_detected, reference_period)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (forecast_date) DO UPDATE SET
            ks_statistic = EXCLUDED.ks_statistic,
            ks_pvalue = EXCLUDED.ks_pvalue,
            drift_detected = EXCLUDED.drift_detected,
            reference_period = EXCLUDED.reference_period
        """, (forecast_date, ks_stat, ks_pval, drift_detected, f"{HISTORY_DAYS}d-glissement"))
    conn.commit()


def compare_with_champion(conn, horizon_hours=72):
    """Compare MAE challenger vs champion sur la dernière évaluation disponible."""
    challenger_query = f"""
    SELECT evaluation_date, mae
    FROM {CHALLENGER_PERF_TABLE}
    WHERE horizon_hours = %s AND mae IS NOT NULL
    ORDER BY evaluation_date DESC
    LIMIT 1
    """
    champion_query = f"""
    SELECT evaluation_date, mae
    FROM {CHAMPION_PERF_TABLE}
    WHERE horizon_hours = %s AND mae IS NOT NULL
    ORDER BY evaluation_date DESC
    LIMIT 1
    """

    challenger_df = pd.read_sql(challenger_query, conn, params=(horizon_hours,), parse_dates=["evaluation_date"])
    champion_df = pd.read_sql(champion_query, conn, params=(horizon_hours,), parse_dates=["evaluation_date"])

    if challenger_df.empty or champion_df.empty:
        return None

    challenger_eval = challenger_df.iloc[0]
    champion_eval = champion_df.iloc[0]

    if challenger_eval["evaluation_date"] != champion_eval["evaluation_date"]:
        return None

    challenger_mae = float(challenger_eval["mae"])
    champion_mae = float(champion_eval["mae"])
    mae_delta = challenger_mae - champion_mae
    winner = MODEL_NAME if challenger_mae < champion_mae else CHAMPION_MODEL_NAME

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {CHALLENGER_COMPARE_TABLE}
                (evaluation_date, horizon_hours, champion_mae, challenger_mae, mae_delta, winner)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (evaluation_date, horizon_hours) DO UPDATE SET
                champion_mae = EXCLUDED.champion_mae,
                challenger_mae = EXCLUDED.challenger_mae,
                mae_delta = EXCLUDED.mae_delta,
                winner = EXCLUDED.winner
            """,
            (
                challenger_eval["evaluation_date"].to_pydatetime(),
                horizon_hours,
                champion_mae,
                challenger_mae,
                mae_delta,
                winner,
            ),
        )
    conn.commit()

    return {
        "evaluation_date": challenger_eval["evaluation_date"],
        "horizon_hours": horizon_hours,
        "champion_mae": champion_mae,
        "challenger_mae": challenger_mae,
        "mae_delta": mae_delta,
        "winner": winner,
    }


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
    print(f"CHALLENGER SARIMA - Consommation Linky (Projet 705) [{MODEL_NAME}]")
    print("=" * 60)

    # --- 1. Connexion PostgreSQL ---
    print("\n[1/11] Connexion PostgreSQL...")
    conn = get_pg_connection()
    print(f"  ✓ Connecté à {PG_DB}@{PG_HOST}")

    # --- 2. Création des tables ---
    print("\n[2/11] Initialisation des tables...")
    create_forecast_table(conn)
    create_performance_table(conn)
    create_drift_table(conn)
    create_model_compare_table(conn)
    create_trigger_table(conn)
    print("  ✓ Tables prêtes")

    # --- 3. Évaluation des TRIGGERS ---
    print("\n[3/11] Évaluation des triggers (décision training)...")
    run_ts, trigger_result = evaluate_run_triggers(conn)
    save_trigger_log(conn, run_ts, trigger_result)
    
    should_train = trigger_result["should_run"] if ENABLE_TRIGGER_GATING else True
    print(f"  Triggers: new_data={trigger_result['trigger_new_data']}, "
          f"weekly={trigger_result['trigger_weekly_retrain']}, "
          f"perf_drop={trigger_result['trigger_perf_drop']}")
    print(f"  → Décision: {'TRAIN' if should_train else 'SKIP TRAINING'}")
    
    if not should_train:
        print("  ⚙ Mode inférence seule (sans entraînement)")

    # === BLOC TRAINING (optionnel) ===
    model = None
    model_order_str = None
    s3_model_uri = None

    if should_train:
        print("\n[4/11] Récupération des données (21j d'historique)...")
        df = fetch_consumption(conn, HISTORY_DAYS)
        print(f"  Points bruts: {len(df)}")
        print(f"  Période: {df.index.min()} → {df.index.max()}")

        df = interpolate_missing_hours(df)
        df = cap_outliers(df)
        series = df["conso_kwh"].values
        print(f"  Points après interpolation: {len(series)}")
        print(f"  Conso moyenne: {series.mean():.4f} kWh/h")

        print("\n[5/11] Détection Data Drift...")
        now_for_drift = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        ks_stat, ks_pval, drift_detected = compute_data_drift(conn, series, HISTORY_DAYS)
        if ks_stat is not None:
            print(f"  KS Statistique: {ks_stat:.4f}")
            print(f"  KS p-value: {ks_pval:.6f}")
            print(f"  Drift détecté: {'✗ OUI' if drift_detected else '✓ NON'}")
            save_drift(conn, now_for_drift, ks_stat, ks_pval, drift_detected)
            send_discord_notification(drift_detected=drift_detected, ks_stat=ks_stat, ks_pval=ks_pval)
        else:
            print("  ⚠ Pas assez de données pour drift detection")

        print("  Profil journalier moyen (kWh/h):")
        hourly_mean = df.groupby(df.index.hour)["conso_kwh"].mean()
        for h in range(24):
            val = hourly_mean.get(h, 0)
            bar = "█" * int(val * 20)
            print(f"    {h:02d}h : {val:.3f} {bar}")

        print(f"\n[6/11] Entraînement SARIMA...")
        model = train_sarima(series)
        model_order_str = f"SARIMA{ORDER}x{SEASONAL_ORDER}"
        print(f"  ✓ Modèle: {model_order_str}")
        print(f"  AIC: {model.aic:.2f}")
        print(f"  BIC: {model.bic:.2f}")

        # Sauvegarde du modèle
        print(f"\n[7/11] Sauvegarde modèle S3 + local...")
        forecast_date = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        s3_model_uri, _ = save_model_to_local_and_s3(model, model_order_str, forecast_date, conn)

        print(f"\n[8/11] Génération de {N_PERIODS}h de prévisions (nouvel entraînement)...")
    else:
        # SKIP training: charger le modèle pré-entraîné
        print("\n[4/11] Saut entraînement → chargement du modèle pré-entraîné...")
        model = load_latest_model()
        forecast_date = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        
        if model is None:
            print("  🛑 Aucun modèle pré-entraîné disponible ET trigger gating actif.")
            print("  Force training requis pour la première exécution.")
            print("\n  → Sortie propre (pas de prédictions sans modèle)")
            conn.close()
            return
        
        print(f"\n[5/11] Génération de {N_PERIODS}h de prévisions (modèle chargé)...")

    # === BLOC INFERENCE (toujours) ===
    # Générer les prévisions avec le modèle (nouveau ou chargé)
    forecast_result = model.get_forecast(steps=N_PERIODS)
    forecast = np.asarray(forecast_result.predicted_mean)
    conf_int = np.asarray(forecast_result.conf_int(alpha=0.20))

    forecast = np.maximum(forecast, 0)
    conf_int[:, 0] = np.maximum(conf_int[:, 0], 0)

    # Déterminer last_ts à partir des données existantes (pas calcul sur df si skip training)
    last_ts_df = pd.read_sql(
        "SELECT MAX(hour) as max_hour FROM dbt_gold.linky_hourly",
        conn,
        parse_dates=["max_hour"]
    )
    last_ts = last_ts_df.iloc[0]["max_hour"].to_pydatetime() if not last_ts_df.empty else forecast_date

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

    if model_order_str is None:
        model_order_str = "SARIMA(2,0,0)(2,1,0,24)"  # Default fallback
    
    print(f"\n[6/11 ou 9/11] Sauvegarde dans {CHALLENGER_FORECAST_TABLE}...")
    saved_forecast_date = save_to_postgres(conn, forecast_df, model_order_str)
    print(f"  ✓ {len(forecast_df)} lignes insérées (forecast_date={saved_forecast_date})")

    # === BLOC EVALUATION (toujours) ===
    print(f"\n[7/11 ou 10/11] Évaluation performance (72h prédites vs 72h réelles)...")
    perf_result = evaluate_previous_forecast(conn, horizon_hours=72)
    if perf_result:
        forecast_date_eval, eval_date, horizon, mae, mse, rmse, mape, coverage, n_points, f_model_order = perf_result
        print(f"  ✓ Données trouvées (forecast_date={forecast_date_eval})")
        print(f"    MAE:  {mae:.4f} kWh/h")
        print(f"    MSE:  {mse:.4f} (kWh/h)^2")
        print(f"    RMSE: {rmse:.4f} kWh/h")
        print(f"    MAPE: {mape:.2f}%" if mape is not None else "    MAPE: n/a")
        print(f"    Coverage 80%: {coverage:.1f}%" if coverage is not None else "    Coverage 80%: n/a")
        save_performance(conn, forecast_date_eval, eval_date, horizon, mae, mse, rmse, mape, coverage, n_points, f_model_order)
    else:
        print("  ⚠ Pas données observées pour évaluation (attendre 72h)")
        perf_result = None

    print(f"\n[8/11 ou 11/11] Comparaison Challenger vs Champion...")
    compare_result = compare_with_champion(conn, horizon_hours=72)
    if compare_result:
        print(
            f"  ✓ MAE champion={compare_result['champion_mae']:.4f}, "
            f"challenger={compare_result['challenger_mae']:.4f}, "
            f"delta={compare_result['mae_delta']:.4f} | winner={compare_result['winner']}"
        )
    else:
        print("  ⚠ Comparaison indisponible (pas fenêtre commune)")

    # === MLflow LOGGING ===
    print("\n[+] Logging MLflow...")
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)
    with mlflow.start_run():
        mlflow.log_params({
            "model_name": MODEL_NAME,
            "training_triggered": str(should_train),
            "order": str(ORDER),
            "seasonal_order": str(SEASONAL_ORDER),
        })
        
        if model is not None:
            mlflow.log_metrics({
                "aic": model.aic,
                "bic": model.bic,
            })
        
        if perf_result:
            forecast_date_eval, eval_date, horizon, mae, mse, rmse, mape, coverage, n_points, _ = perf_result
            mlflow.log_metrics({
                f"performance_last{horizon}h_mae_kwh": mae if mae is not None else 0,
                f"performance_last{horizon}h_rmse_kwh": rmse if rmse is not None else 0,
            })
        
        if compare_result:
            mlflow.log_metrics({
                "comparison_mae_delta": float(compare_result["mae_delta"]),
            })
            mlflow.log_param("comparison_winner", compare_result["winner"])

        if model is not None:
            try:
                mlflow.statsmodels.log_model(model, artifact_path="model")
            except Exception as exc:
                print(f"  ⚠ MLflow model logging échoué: {exc}")
    
    print("  ✓ Run MLflow enregistré")

    conn.close()
    print("\n" + "=" * 60)
    print(f"✓ TERMINÉ — Mode={'TRAINING' if should_train else 'INFERENCE'}")
    print("=" * 60)


if __name__ == "__main__":
    main()
