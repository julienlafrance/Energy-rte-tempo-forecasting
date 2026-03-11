#!/usr/bin/env python3
"""
mlops_forecast_linky_705.py - Inférence consommation Linky pour Projet 705

Source : PostgreSQL dbt_gold.linky_hourly
Modèle : chargé depuis MLflow (Registry ou dernier run d'entraînement)
Output : PostgreSQL gold.mlops_linky_forecast
Tracking : MLflow

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
# ]
# ///

import os
import warnings
from datetime import datetime, timedelta

import mlflow
import numpy as np
import pandas as pd
import psycopg2
from mlflow.tracking import MlflowClient
from psycopg2.extras import execute_values
from scipy.stats import ks_2samp
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


def create_forecast_table(conn):
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
    now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    forecast_df_clean = forecast_df[~forecast_df.index.duplicated(keep="last")].sort_index()

    rows = []
    hours_to_replace = []
    for _, row in forecast_df_clean.iterrows():
        hour_ts = row.name.to_pydatetime()
        hours_to_replace.append(hour_ts)
        rows.append(
            (
                now,
                hour_ts,
                float(row["predicted"]),
                float(row["lower"]),
                float(row["upper"]),
                model_order_str,
            )
        )

    if not rows:
        return now

    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM gold.mlops_linky_forecast WHERE hour = ANY(%s)",
            (hours_to_replace,),
        )
        execute_values(
            cur,
            """INSERT INTO gold.mlops_linky_forecast
               (forecast_date, hour, conso_kwh_predicted, conso_kwh_lower, conso_kwh_upper, model_order)
               VALUES %s""",
            rows,
        )
    conn.commit()
    return now


def evaluate_previous_forecast(conn, horizon_hours=72):
    now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT forecast_date
            FROM gold.mlops_linky_forecast
            WHERE forecast_date <= %s - (%s * INTERVAL '1 hour')
            ORDER BY forecast_date DESC
            LIMIT 1
            """,
            (now, horizon_hours),
        )
        result = cur.fetchone()

    if not result:
        return None

    forecast_date = result[0]
    window_start = forecast_date
    window_end = forecast_date + timedelta(hours=horizon_hours)

    pred_df = pd.read_sql(
        """SELECT hour, conso_kwh_predicted, conso_kwh_lower, conso_kwh_upper, model_order
           FROM gold.mlops_linky_forecast
           WHERE forecast_date = %s
             AND hour > %s AND hour <= %s
           ORDER BY hour""",
        conn,
        params=(forecast_date, window_start, window_end),
        parse_dates=["hour"],
    )
    if pred_df.empty:
        return None

    real_df = pd.read_sql(
        """SELECT hour, SUM(consommation_kwh) as conso_kwh
           FROM dbt_gold.linky_hourly
           WHERE hour > %s AND hour <= %s
           GROUP BY hour
           ORDER BY hour""",
        conn,
        params=(window_start, window_end),
        parse_dates=["hour"],
    )
    if real_df.empty:
        return None

    merged = pd.merge(pred_df, real_df, on="hour", how="inner")
    if merged.empty:
        return None

    merged = merged.replace([np.inf, -np.inf], np.nan)
    merged_core = merged.dropna(subset=["conso_kwh", "conso_kwh_predicted"])
    if merged_core.empty:
        return None

    y_true = merged_core["conso_kwh"].values
    y_pred = merged_core["conso_kwh_predicted"].values
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
            (coverage_base["conso_kwh"] >= coverage_base["conso_kwh_lower"])
            & (coverage_base["conso_kwh"] <= coverage_base["conso_kwh_upper"])
        ) * 100

    model_order = pred_df["model_order"].iloc[0]
    return forecast_date, now, horizon_hours, mae, mse, rmse, mape, coverage, len(merged_core), model_order


def save_performance(conn, forecast_date, evaluation_date, horizon_hours, mae, mse, rmse, mape, coverage, n_points, model_order):
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
        cur.execute(
            """
            INSERT INTO gold.mlops_linky_performance
            (forecast_date, evaluation_date, horizon_hours, mae, mse, rmse, mape, coverage_80, n_points, model_order)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (forecast_date, evaluation_date, horizon_hours) DO UPDATE SET
                mae = EXCLUDED.mae,
                mse = EXCLUDED.mse,
                rmse = EXCLUDED.rmse,
                mape = EXCLUDED.mape,
                coverage_80 = EXCLUDED.coverage_80,
                n_points = EXCLUDED.n_points,
                model_order = EXCLUDED.model_order
            """,
            (forecast_date, evaluation_date, horizon_hours, mae, mse, rmse, mape, coverage, n_points, model_order),
        )
    conn.commit()


def compute_data_drift(conn, current_series, history_days=HISTORY_DAYS):
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

    ks_stat, ks_pval = ks_2samp(current_series, previous_series)
    drift_detected = ks_pval < 0.05
    return float(ks_stat), float(ks_pval), drift_detected


def save_drift(conn, forecast_date, ks_stat, ks_pval, drift_detected):
    ks_stat = None if (ks_stat is None or np.isnan(ks_stat)) else float(ks_stat)
    ks_pval = None if (ks_pval is None or np.isnan(ks_pval)) else float(ks_pval)
    drift_detected = bool(drift_detected) if drift_detected is not None else False

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO gold.mlops_linky_drift
            (forecast_date, ks_statistic, ks_pvalue, drift_detected, reference_period)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (forecast_date) DO UPDATE SET
                ks_statistic = EXCLUDED.ks_statistic,
                ks_pvalue = EXCLUDED.ks_pvalue,
                drift_detected = EXCLUDED.drift_detected,
                reference_period = EXCLUDED.reference_period
            """,
            (forecast_date, ks_stat, ks_pval, drift_detected, f"{HISTORY_DAYS}d-glissement"),
        )
    conn.commit()


def load_latest_trained_model():
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient()

    if MLFLOW_REGISTERED_MODEL_NAME:
        model_uri = f"models:/{MLFLOW_REGISTERED_MODEL_NAME}/latest"
        try:
            model = mlflow.statsmodels.load_model(model_uri)
            return model, model_uri
        except Exception:
            pass

    experiment = client.get_experiment_by_name(MLFLOW_EXPERIMENT)
    if experiment is None:
        raise RuntimeError(f"Expérience MLflow introuvable: {MLFLOW_EXPERIMENT}")

    runs = client.search_runs(
        [experiment.experiment_id],
        filter_string="tags.phase = 'train'",
        order_by=["attributes.start_time DESC"],
        max_results=20,
    )

    for run in runs:
        run_model_uri = f"runs:/{run.info.run_id}/model"
        try:
            model = mlflow.statsmodels.load_model(run_model_uri)
            return model, run_model_uri
        except Exception:
            continue

    raise RuntimeError("Aucun modèle entraîné exploitable trouvé dans MLflow")


def main():
    print("=" * 60)
    print("INFÉRENCE SARIMA - Consommation Linky (Projet 705)")
    print("=" * 60)

    print("\n[1/6] Connexion PostgreSQL...")
    conn = get_pg_connection()
    print(f"  ✓ Connecté à {PG_DB}@{PG_HOST}")

    print("\n[2/7] Évaluation performance du dernier forecast complet...")
    create_performance_table(conn)
    perf_result = evaluate_previous_forecast(conn, horizon_hours=N_PERIODS)
    perf_metrics = {}
    if perf_result:
        forecast_date_prev, eval_date, horizon, mae, mse, rmse, mape, coverage, n_points, model_order = perf_result
        save_performance(conn, *perf_result)
        print("  ✓ Performance rolling mise à jour")
        print(f"    MAE={mae:.4f}, MSE={mse:.4f}, RMSE={rmse:.4f}")
        if mape is not None:
            print(f"    MAPE={mape:.2f}%")
        if coverage is not None:
            print(f"    Coverage80={coverage:.1f}% ({n_points} points)")

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
    else:
        print("  ⚠ Pas encore de données observées pour évaluation")

    print(f"\n[3/7] Récupération des {HISTORY_DAYS} derniers jours...")
    df = fetch_consumption(conn, HISTORY_DAYS)
    df = interpolate_missing_hours(df)
    df = cap_outliers(df)
    series = df["conso_kwh"]
    print(f"  ✓ Points utilisés: {len(series)}")

    print("\n[4/7] Détection data drift (Kolmogorov-Smirnov)...")
    create_drift_table(conn)
    ks_stat, ks_pval, drift_detected = compute_data_drift(conn, series.values, HISTORY_DAYS)

    forecast_date = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    save_drift(conn, forecast_date, ks_stat, ks_pval, drift_detected)
    if ks_stat is None:
        print("  ⚠ Drift non calculable (historique de référence insuffisant)")
    else:
        print(f"  ✓ KS={ks_stat:.4f}, p-value={ks_pval:.6g}, drift={drift_detected}")

    print("\n[5/7] Chargement du modèle entraîné...")
    trained_model, loaded_model_uri = load_latest_trained_model()
    print(f"  ✓ Modèle chargé depuis {loaded_model_uri}")

    print(f"\n[6/7] Génération de {N_PERIODS}h de prévisions...")
    inference_model = SARIMAX(
        series,
        order=ORDER,
        seasonal_order=SEASONAL_ORDER,
        enforce_stationarity=False,
        enforce_invertibility=False,
    ).filter(trained_model.params)

    forecast_result = inference_model.get_forecast(steps=N_PERIODS)
    forecast = np.asarray(forecast_result.predicted_mean)
    conf_int = np.asarray(forecast_result.conf_int(alpha=0.20))

    forecast = np.maximum(forecast, 0)
    conf_int[:, 0] = np.maximum(conf_int[:, 0], 0)

    last_ts = df.index.max()
    forecast_index = pd.date_range(start=last_ts + timedelta(hours=1), periods=N_PERIODS, freq="h")
    forecast_df = pd.DataFrame(
        {
            "predicted": forecast,
            "lower": conf_int[:, 0],
            "upper": conf_int[:, 1],
        },
        index=forecast_index,
    )

    print("\n[7/7] Sauvegarde forecast + tracking MLflow...")
    create_forecast_table(conn)
    model_order_str = f"SARIMA{ORDER}x{SEASONAL_ORDER}"
    forecast_date = save_to_postgres(conn, forecast_df, model_order_str)
    print(f"  ✓ {len(forecast_df)} lignes insérées (forecast_date={forecast_date})")

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)
    forecast_run_name = f"forecast_{forecast_date.strftime('%Y%m%d_%H%M%S')}"
    with mlflow.start_run(run_name=forecast_run_name) as run:
        mlflow.set_tag("phase", "forecast")
        mlflow.log_params(
            {
                "model_uri_used": loaded_model_uri,
                "order": str(ORDER),
                "seasonal_order": str(SEASONAL_ORDER),
                "history_days": HISTORY_DAYS,
                "n_periods": N_PERIODS,
                "n_points": len(series),
                "forecast_date": forecast_date.isoformat(),
                "source": "postgresql/dbt_gold.linky_hourly",
            }
        )
        metrics = {
            "mean_consumption": float(series.mean()),
            "std_consumption": float(series.std()),
            "mean_forecast": float(forecast.mean()),
            "ks_statistic": float(ks_stat) if ks_stat is not None else np.nan,
            "ks_pvalue": float(ks_pval) if ks_pval is not None else np.nan,
            "drift_detected": 1.0 if drift_detected else 0.0,
        }
        metrics.update(perf_metrics)
        mlflow.log_metrics(metrics)
        print(f"  ✓ MLflow run_id={run.info.run_id}, run_name={forecast_run_name}")
        print(f"  ✓ Metrics loggées: {metrics}")

    conn.close()
    print("\n" + "=" * 60)
    print(f"✓ TERMINÉ — {model_order_str}, {N_PERIODS}h de prévisions générées")
    print("=" * 60)


if __name__ == "__main__":
    main()
