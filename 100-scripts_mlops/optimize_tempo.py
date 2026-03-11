#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "psycopg2-binary",
#     "pandas",
#     "scipy",
#     "numpy",
# ]
# ///
"""
Optimisation des paramètres de l'algorithme Tempo RTE.

Approche hybride :
  1. Normalisation analytique (quantiles q40/q80 des données réelles)
  2. Optimisation Nelder-Mead des 6 coefficients de seuils (~2 secondes)

Usage:
    cd /opt/projet705/scripts && uv run optimize_tempo.py
"""

import os
import warnings
import numpy as np
import pandas as pd
import psycopg2
from scipy.optimize import minimize
from datetime import date
from time import time

warnings.filterwarnings("ignore", category=UserWarning)

PG_CONFIG = {
    "host": os.environ.get("PG_HOST", "projet-db"),
    "dbname": os.environ.get("PG_DB", "airflow"),
    "user": os.environ.get("PG_USER", "airflow"),
    "password": os.environ.get("PG_PASS", "airflow"),
}

RTE_PARAMS = {
    "norm_centre": 46050,
    "norm_echelle": 2160,
    "sbr_intercept": 4.00,
    "sbr_coeff_jour": 0.015,
    "sbr_coeff_stock": 0.026,
    "sr_intercept": 3.15,
    "sr_coeff_jour": 0.010,
    "sr_coeff_stock": 0.031,
}

TEMPO_START = date(2025, 9, 1)


def load_data():
    conn = psycopg2.connect(**PG_CONFIG)
    df_conso = pd.read_sql("""
        SELECT d.date,
            avg(c.avg_value_mw) AS conso_brute_mw,
            coalesce(avg(CASE WHEN g.production_type = 'WIND' THEN g.avg_value END), 0) AS eolien_mw,
            coalesce(avg(CASE WHEN g.production_type = 'SOLAR' THEN g.avg_value END), 0) AS pv_mw
        FROM raw.rte_tempo_calendar d
        JOIN silver.rte_consumption_hourly c
            ON c.ts_hour >= (d.date::timestamp + interval '6 hours') AT TIME ZONE 'Europe/Paris'
           AND c.ts_hour <  (d.date::timestamp + interval '30 hours') AT TIME ZONE 'Europe/Paris'
        LEFT JOIN gold.rte_hourly g
            ON g.ts_hour >= (d.date::timestamp + interval '6 hours') AT TIME ZONE 'Europe/Paris'
           AND g.ts_hour <  (d.date::timestamp + interval '30 hours') AT TIME ZONE 'Europe/Paris'
           AND g.production_type IN ('WIND', 'SOLAR')
        GROUP BY d.date ORDER BY d.date
    """, conn)
    df_conso["conso_nette_mw"] = df_conso["conso_brute_mw"] - df_conso["eolien_mw"] - df_conso["pv_mw"]

    df_tempo = pd.read_sql("SELECT date, color FROM raw.rte_tempo_calendar ORDER BY date", conn)
    conn.close()

    df = df_conso.merge(df_tempo, on="date")
    df["date"] = pd.to_datetime(df["date"])
    df["dow"] = df["date"].dt.dayofweek
    df["month"] = df["date"].dt.month
    df["jour_tempo"] = (df["date"].dt.date - TEMPO_START).apply(lambda x: x.days + 1)

    df = df.sort_values("date").reset_index(drop=True)
    rouge_used, blanc_used = 0, 0
    stock_r, stock_b = [], []
    for _, row in df.iterrows():
        stock_r.append(22 - rouge_used)
        stock_b.append(43 - blanc_used)
        if row["color"] == "RED": rouge_used += 1
        elif row["color"] == "WHITE": blanc_used += 1
    df["stock_rouge"], df["stock_blanc"] = stock_r, stock_b

    print(f"Données : {len(df)} jours ({df['date'].min().date()} → {df['date'].max().date()})")
    print(f"  BLUE={len(df[df['color']=='BLUE'])}, WHITE={len(df[df['color']=='WHITE'])}, RED={len(df[df['color']=='RED'])}")
    return df


def apply_algorithm(df, params):
    nc, ne, sbr_i, sbr_j, sbr_s, sr_i, sr_j, sr_s = params
    conso_std = (df["conso_nette_mw"] - nc) / ne
    seuil_br = sbr_i - sbr_j * df["jour_tempo"] - sbr_s * (df["stock_rouge"] + df["stock_blanc"])
    seuil_r = sr_i - sr_j * df["jour_tempo"] - sr_s * df["stock_rouge"]

    couleurs = pd.Series("BLUE", index=df.index)
    couleurs[(conso_std > seuil_br) & (df["dow"] != 6)] = "WHITE"
    couleurs[(conso_std > seuil_r) & df["month"].isin([11,12,1,2,3]) & ~df["dow"].isin([5,6])] = "RED"
    return couleurs


def score(params, df):
    return int((apply_algorithm(df, params) != df["color"]).sum())


def confusion(df, params, label=""):
    calc = apply_algorithm(df, params)
    match = calc == df["color"]
    total, ok = len(df), int(match.sum())
    pct = 100 * ok / total

    print(f"\n  {label}")
    print(f"  Concordance : {ok}/{total} = {pct:.1f}%\n")
    print(f"  {'':>14} | Off. BLUE | Off. WHITE | Off. RED")
    print(f"  {'-'*14}-+-{'-'*10}-+-{'-'*11}-+-{'-'*9}")
    for c in ["BLUE", "WHITE", "RED"]:
        vals = [int(((calc == c) & (df["color"] == o)).sum()) for o in ["BLUE", "WHITE", "RED"]]
        print(f"  {'Calc ' + c:>14} | {vals[0]:>9} | {vals[1]:>10} | {vals[2]:>8}")
    print(f"\n  Rappel :")
    for c in ["BLUE", "WHITE", "RED"]:
        m = df["color"] == c
        if m.sum():
            print(f"    {c:>5} : {int((match&m).sum())}/{int(m.sum())} = {100*int((match&m).sum())/int(m.sum()):.0f}%")
    return pct


def main():
    df = load_data()

    # --- RTE originaux ---
    rte = list(RTE_PARAMS.values())
    print("\n" + "=" * 60)
    print("  PARAMÈTRES RTE ORIGINAUX")
    print("=" * 60)
    for k, v in RTE_PARAMS.items():
        print(f"  {k:>20} = {v}")
    confusion(df, rte, "Résultats RTE originaux")

    # --- Optimisation hybride ---
    print("\n" + "=" * 60)
    print("  OPTIMISATION HYBRIDE")
    print("=" * 60)

    # Étape 1 : normalisation analytique
    q40 = float(df["conso_nette_mw"].quantile(0.4))
    q80 = float(df["conso_nette_mw"].quantile(0.8))
    nc, ne = q40, q80 - q40
    print(f"  Normalisation : centre={nc:.0f} MW, échelle={ne:.0f} MW")

    # Étape 2 : Nelder-Mead sur les 6 coefficients de seuils
    t0 = time()
    result = minimize(
        lambda s: score([nc, ne, *s], df),
        x0=[4.00, 0.015, 0.026, 3.15, 0.010, 0.031],
        method="Nelder-Mead",
        options={"maxiter": 10000, "xatol": 1e-8, "fatol": 0.5},
    )
    elapsed = time() - t0

    opt = [nc, ne] + list(result.x)
    names = list(RTE_PARAMS.keys())

    print(f"  Terminé en {elapsed:.1f}s — {int(result.fun)} erreurs\n")
    print(f"  Paramètres optimisés :")
    for name, v_rte, v_opt in zip(names, rte, opt):
        delta = ((v_opt - v_rte) / v_rte * 100) if v_rte else 0
        print(f"    {name:>20} : {v_rte:>10.4f} → {v_opt:>10.4f}  ({delta:+.1f}%)")

    pct_opt = confusion(df, opt, "Résultats optimisés")
    pct_rte = 100 * (len(df) - score(rte, df)) / len(df)

    print(f"\n  RTE originaux : {pct_rte:.1f}% → Optimisés : {pct_opt:.1f}% (+{pct_opt-pct_rte:.1f})")

    # --- Écriture en base ---
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO gold.tempo_params
            (norm_centre, norm_echelle, sbr_intercept, sbr_coeff_jour, sbr_coeff_stock,
             sr_intercept, sr_coeff_jour, sr_coeff_stock, pct_match, total_jours)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (*[float(x) for x in opt], float(pct_opt), len(df)))
    conn.commit()
    cur.close()
    conn.close()
    print(f"  → Injecté dans gold.tempo_params")


if __name__ == "__main__":
    main()
