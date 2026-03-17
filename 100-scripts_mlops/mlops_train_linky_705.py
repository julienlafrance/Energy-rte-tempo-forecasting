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
MLFLOW_RUN_NAME_PREFIX = os.environ.get("MLFLOW_RUN_NAME_PREFIX", "training_linky_sarima_705")
MLFLOW_REGISTERED_MODEL_NAME = os.environ.get("MLFLOW_REGISTERED_MODEL_NAME", "linky_sarima_705")

MODEL_REGISTRY_S3_BUCKET = os.environ.get("MODEL_REGISTRY_S3_BUCKET", "705")
MODEL_REGISTRY_S3_PREFIX = os.environ.get("MODEL_REGISTRY_S3_PREFIX", "mlops/linky-sarima-705")
MODEL_REGISTRY_S3_ENDPOINT_URL = os.environ.get(
    "MODEL_REGISTRY_S3_ENDPOINT_URL",
    os.environ.get("MLFLOW_S3_ENDPOINT_URL", ""),
)

ORDER = (2, 0, 0)
SEASONAL_ORDER = (2, 1, 0, 24)
SARIMA_CANDIDATES = [
    ((1, 0, 0), (1, 1, 0, 24)),
    ((2, 0, 0), (1, 1, 0, 24)),
    ((2, 0, 0), (2, 1, 0, 24)),
    ((2, 0, 1), (2, 1, 0, 24)),
    ((3, 0, 0), (2, 1, 0, 24)),
]
HISTORY_DAYS = 21
N_PERIODS = 72
TRAIN_RATIO = 0.70  # 70% entraînement / 30% test


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


def train_best_sarima(train_series, candidates, test_series=None):
    print(f"  {len(candidates)} configurations SARIMA à évaluer")
    best_model = None
    best_order = None
    best_seasonal_order = None
    best_aic = float("inf")
    training_results = []

    n_test = len(test_series) if test_series is not None else 0

    for idx, (order, seasonal_order) in enumerate(candidates, start=1):
        model_name = f"SARIMA{order}x{seasonal_order}"
        print(f"  [{idx}/{len(candidates)}] {model_name}")
        try:
            model = SARIMAX(
                train_series,
                order=order,
                seasonal_order=seasonal_order,
                enforce_stationarity=False,
                enforce_invertibility=False,
            )
            fitted = model.fit(disp=False)
            aic = float(fitted.aic)
            bic = float(fitted.bic)

            # Évaluation sur le jeu de test (30%)
            test_mae = float("nan")
            test_rmse = float("nan")
            test_mape = float("nan")
            if test_series is not None and n_test > 0:
                try:
                    fc = fitted.get_forecast(steps=n_test)
                    y_pred = np.maximum(np.asarray(fc.predicted_mean), 0)
                    y_true = np.asarray(test_series)
                    errors = y_true - y_pred
                    test_mae = float(np.mean(np.abs(errors)))
                    test_rmse = float(np.sqrt(np.mean(errors ** 2)))
                    non_zero = y_true > 0
                    if np.any(non_zero):
                        test_mape = float(
                            np.mean(np.abs(errors[non_zero] / y_true[non_zero])) * 100
                        )
                except Exception as exc_test:
                    print(f"      ⚠ Évaluation test échouée: {exc_test}")

            training_results.append(
                {
                    "candidate_idx": idx,
                    "order": order,
                    "seasonal_order": seasonal_order,
                    "aic": aic,
                    "bic": bic,
                    "test_mae": test_mae,
                    "test_rmse": test_rmse,
                    "test_mape": test_mape,
                    "status": "success",
                    "error": "",
                }
            )
            mae_str = f" | test_MAE={test_mae:.4f} RMSE={test_rmse:.4f}" if n_test > 0 else ""
            print(f"      AIC={aic:.2f} | BIC={bic:.2f}{mae_str}")

            if aic < best_aic:
                best_aic = aic
                best_model = fitted
                best_order = order
                best_seasonal_order = seasonal_order
        except Exception as exc:
            training_results.append(
                {
                    "candidate_idx": idx,
                    "order": order,
                    "seasonal_order": seasonal_order,
                    "aic": float("nan"),
                    "bic": float("nan"),
                    "test_mae": float("nan"),
                    "test_rmse": float("nan"),
                    "test_mape": float("nan"),
                    "status": "failed",
                    "error": str(exc),
                }
            )
            print(f"      ⚠ Échec entraînement {model_name}: {exc}")

    if best_model is None:
        raise RuntimeError("Aucun modèle SARIMA n'a pu être entraîné correctement")

    print(f"  ✓ Meilleur modèle: SARIMA{best_order}x{best_seasonal_order} (AIC={best_aic:.2f})")
    return best_model, best_order, best_seasonal_order, training_results


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
    s3_client.put_object(Bucket=MODEL_REGISTRY_S3_BUCKET, Key=model_key, Body=model_buffer.getvalue())
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
    print(f"  ✓ Points disponibles: {len(series)}")

    # Split temporel 70% train / 30% test
    n_train = int(len(series) * TRAIN_RATIO)
    n_test = len(series) - n_train
    train_series = series[:n_train]
    test_series = series[n_train:]
    print(f"  ✓ Split {int(TRAIN_RATIO*100)}/{int((1-TRAIN_RATIO)*100)}: train={n_train} pts, test={n_test} pts")

    print("\n[3/4] Entraînement modèle (sur jeu train 70%)...")
    model, best_order, best_seasonal_order, training_results = train_best_sarima(
        train_series, SARIMA_CANDIDATES, test_series
    )
    model_order_str = f"SARIMA{best_order}x{best_seasonal_order}"
    train_date = datetime.utcnow().replace(minute=0, second=0, microsecond=0)

    # Métriques test du meilleur modèle
    best_result = next(
        (r for r in training_results
         if r["order"] == best_order and r["seasonal_order"] == best_seasonal_order),
        {},
    )
    test_mae = best_result.get("test_mae", float("nan"))
    test_rmse = best_result.get("test_rmse", float("nan"))
    test_mape = best_result.get("test_mape", float("nan"))

    print(f"  ✓ Meilleur modèle : {model_order_str}")
    print(f"  AIC (train): {model.aic:.2f}")
    print(f"  BIC (train): {model.bic:.2f}")
    if not np.isnan(test_mae):
        print(f"  Test MAE : {test_mae:.4f} kWh/h")
        print(f"  Test RMSE: {test_rmse:.4f} kWh/h")
        if not np.isnan(test_mape):
            print(f"  Test MAPE: {test_mape:.2f}%")

    # Réentraînement sur la série complète avant mise en production
    print(f"\n  Réentraînement sur série complète ({len(series)} pts)...")
    final_model = SARIMAX(
        series,
        order=best_order,
        seasonal_order=best_seasonal_order,
        enforce_stationarity=False,
        enforce_invertibility=False,
    ).fit(disp=False)
    model = final_model  # modèle final sauvegardé dans MLflow
    print(f"  ✓ Réentraînement terminé — AIC={model.aic:.2f}, BIC={model.bic:.2f}")

    print("\n[4/4] Tracking MLflow + registry...")
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    s3_model_uri = register_model_to_s3(model, model_order_str, train_date)

    run_name = f"{MLFLOW_RUN_NAME_PREFIX}_{train_date.strftime('%Y%m%d%H')}"
    with mlflow.start_run(run_name=run_name):
        mlflow.set_tag("phase", "train")
        mlflow.log_param("run_name", run_name)
        mlflow.log_params(
            {
                "order": str(best_order),
                "seasonal_order": str(best_seasonal_order),
                "sarima_candidates_count": len(SARIMA_CANDIDATES),
                "best_model_order": str(best_order),
                "best_model_seasonal_order": str(best_seasonal_order),
                "best_model_name": model_order_str,
                "history_days": HISTORY_DAYS,
                "n_periods": N_PERIODS,
                "n_points": len(series),
                "n_train": n_train,
                "n_test": n_test,
                "train_ratio": TRAIN_RATIO,
                "source": "postgresql/dbt_gold.linky_hourly",
                "model_registry_s3_bucket": MODEL_REGISTRY_S3_BUCKET or "",
                "model_registry_s3_prefix": MODEL_REGISTRY_S3_PREFIX,
            }
        )

        for result in training_results:
            idx = result["candidate_idx"]
            mlflow.log_param(f"candidate_{idx}_order", str(result["order"]))
            mlflow.log_param(f"candidate_{idx}_seasonal_order", str(result["seasonal_order"]))
            mlflow.log_param(f"candidate_{idx}_status", result["status"])
            if result["error"]:
                mlflow.log_param(f"candidate_{idx}_error", result["error"][:240])
            mlflow.log_metric(f"candidate_{idx}_aic", float(result["aic"]))
            mlflow.log_metric(f"candidate_{idx}_bic", float(result["bic"]))
            if not np.isnan(result.get("test_mae", float("nan"))):
                mlflow.log_metric(f"candidate_{idx}_test_mae", float(result["test_mae"]))
                mlflow.log_metric(f"candidate_{idx}_test_rmse", float(result["test_rmse"]))
                if not np.isnan(result.get("test_mape", float("nan"))):
                    mlflow.log_metric(f"candidate_{idx}_test_mape", float(result["test_mape"]))

        test_metrics = {
            "aic": model.aic,
            "bic": model.bic,
            "mean_consumption": float(np.mean(series)),
            "std_consumption": float(np.std(series)),
        }
        if not np.isnan(test_mae):
            test_metrics["test_mae"] = test_mae
            test_metrics["test_rmse"] = test_rmse
            if not np.isnan(test_mape):
                test_metrics["test_mape"] = test_mape
        mlflow.log_metrics(test_metrics)

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
