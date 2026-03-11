#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "psycopg2-binary",
#     "pandas",
#     "numpy",
#     "scipy",
# ]
# ///
"""
optimize_tempo_v2.py — Recalibrage des seuils Tempo avec contraintes Open DPE
==============================================================================
Open DPE montre que le gap moyen blanc↔rouge devrait être ~0.47 (≈6000 MW).
Notre modèle actuel a un gap de ~0.17 (≈2700 MW) — trop serré.

Le problème : norm_echelle = q80-q40 = 15751 MW est trop large.
Open DPE utilise une échelle plus petite (~12800 MW) qui sépare mieux les couleurs.

Ce script teste 3 normalisations et optimise les seuils pour chacune,
avec une contrainte sur le gap minimum.

Usage :
  export PG_HOST=172.18.0.3 PG_DB=airflow PG_USER=airflow PG_PASS=airflow
  uv run python3 optimize_tempo_v2.py
"""

import os, warnings
import numpy as np
import pandas as pd
import psycopg2
from scipy.optimize import minimize, differential_evolution
from datetime import date
from time import time

warnings.filterwarnings("ignore")

PG_CONFIG = {
    "host":     os.environ.get("PG_HOST", "projet-db"),
    "dbname":   os.environ.get("PG_DB",   "airflow"),
    "user":     os.environ.get("PG_USER", "airflow"),
    "password": os.environ.get("PG_PASS", "airflow"),
}
TEMPO_START = date(2025, 9, 1)

# ── Référence Open DPE (saison 2024-2025) ────────────────────────────────────
# Seuils typiques Open DPE : blanc+rouge ≈ 1.0-2.0, rouge ≈ 1.5-2.3
# Gap moyen ≈ 0.47 (6000 MW), échelle ≈ 12800 MW
OPENDPE_REF = {
    "gap_moyen_cible": 0.45,     # gap BR↔R cible
    "gap_min":         0.30,     # gap minimum acceptable
    "seuil_br_sept":   2.00,     # seuil blanc+rouge en septembre
    "seuil_r_sept":    2.30,     # seuil rouge en septembre
}

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
    df["dow"]  = df["date"].dt.dayofweek
    df["month"] = df["date"].dt.month
    df["jour_tempo"] = (df["date"].dt.date - TEMPO_START).apply(lambda x: x.days + 1)
    df = df.sort_values("date").reset_index(drop=True)

    rouge_used, blanc_used = 0, 0
    stock_r, stock_b = [], []
    for _, row in df.iterrows():
        stock_r.append(22 - rouge_used)
        stock_b.append(43 - blanc_used)
        if row["color"] == "RED":   rouge_used += 1
        elif row["color"] == "WHITE": blanc_used += 1
    df["stock_rouge"], df["stock_blanc"] = stock_r, stock_b

    print(f"Données : {len(df)} jours ({df['date'].min().date()} → {df['date'].max().date()})")
    vc = df["color"].value_counts()
    print(f"  BLUE={vc.get('BLUE',0)}, WHITE={vc.get('WHITE',0)}, RED={vc.get('RED',0)}")
    return df

def apply_algorithm(df, params):
    nc, ne, sbr_i, sbr_j, sbr_s, sr_i, sr_j, sr_s = params
    conso_std = (df["conso_nette_mw"] - nc) / ne
    seuil_br = sbr_i - sbr_j * df["jour_tempo"] - sbr_s * (df["stock_rouge"] + df["stock_blanc"])
    seuil_r  = sr_i  - sr_j  * df["jour_tempo"] - sr_s  * df["stock_rouge"]

    couleurs = pd.Series("BLUE", index=df.index)
    couleurs[(conso_std > seuil_br) & (df["dow"] != 6)] = "WHITE"
    couleurs[(conso_std > seuil_r) & df["month"].isin([11,12,1,2,3]) & ~df["dow"].isin([5,6])] = "RED"
    return couleurs, seuil_br, seuil_r

def score_with_penalty(params, df, gap_min=0.30):
    """Score = erreurs + pénalité si gap moyen < gap_min."""
    nc, ne, sbr_i, sbr_j, sbr_s, sr_i, sr_j, sr_s = params
    couleurs, seuil_br, seuil_r = apply_algorithm(df, params)
    errors = int((couleurs != df["color"]).sum())

    # Pénalité sur le gap moyen
    gap = (seuil_r - seuil_br).mean()
    if gap < gap_min:
        penalty = 50 * (gap_min - gap) ** 2  # forte pénalité
    else:
        penalty = 0

    # Pénalité si échelle trop grande (> 14000 MW)
    if ne > 14000:
        penalty += 10 * ((ne - 14000) / 1000) ** 2

    return errors + penalty

def confusion(df, params, label=""):
    calc, seuil_br, seuil_r = apply_algorithm(df, params)
    match = calc == df["color"]
    total, ok = len(df), int(match.sum())
    pct = 100 * ok / total

    gap = (seuil_r - seuil_br).mean()
    print(f"\n  {label}")
    print(f"  Concordance : {ok}/{total} = {pct:.1f}%")
    print(f"  Gap moyen BR↔R : {gap:.3f}  (cible ≥ 0.30, Open DPE ≈ 0.47)")
    print(f"  Normalisation : centre={params[0]:.0f} MW, échelle={params[1]:.0f} MW\n")

    print(f"  {'':>14} | Off. BLUE | Off. WHITE | Off. RED")
    print(f"  {'-'*14}-+-{'-'*10}-+-{'-'*11}-+-{'-'*9}")
    for c in ["BLUE", "WHITE", "RED"]:
        vals = [int(((calc == c) & (df["color"] == o)).sum()) for o in ["BLUE", "WHITE", "RED"]]
        print(f"  {'Calc ' + c:>14} | {vals[0]:>9} | {vals[1]:>10} | {vals[2]:>8}")

    print(f"\n  Rappel :")
    for c in ["BLUE", "WHITE", "RED"]:
        m = df["color"] == c
        if m.sum():
            r = int((match & m).sum())
            print(f"    {c:>5} : {r}/{int(m.sum())} = {100*r/int(m.sum()):.0f}%")
    return pct

def main():
    df = load_data()
    cn = df["conso_nette_mw"]

    # ══════════════════════════════════════════════════════════════════════
    # NORMALISATION 1 — Actuelle (q40/q80)
    # ══════════════════════════════════════════════════════════════════════
    nc1 = float(cn.quantile(0.40))
    ne1 = float(cn.quantile(0.80) - cn.quantile(0.40))
    print(f"\n{'='*60}")
    print(f"  NORM 1 — q40/q80 (actuel)")
    print(f"  centre={nc1:.0f}  échelle={ne1:.0f}")
    print(f"{'='*60}")

    t0 = time()
    r1 = minimize(
        lambda s: score_with_penalty([nc1, ne1, *s], df, gap_min=0.30),
        x0=[4.00, 0.015, 0.026, 3.15, 0.010, 0.031],
        method="Nelder-Mead",
        options={"maxiter": 20000, "xatol": 1e-8, "fatol": 0.5},
    )
    opt1 = [nc1, ne1] + list(r1.x)
    pct1 = confusion(df, opt1, "Norm q40/q80 optimisée")
    print(f"  ({time()-t0:.1f}s)")

    # ══════════════════════════════════════════════════════════════════════
    # NORMALISATION 2 — Open DPE style (mean / std)
    # ══════════════════════════════════════════════════════════════════════
    nc2 = float(cn.mean())
    ne2 = float(cn.std())
    print(f"\n{'='*60}")
    print(f"  NORM 2 — mean/std (Open DPE style)")
    print(f"  centre={nc2:.0f}  échelle={ne2:.0f}")
    print(f"{'='*60}")

    t0 = time()
    # Initialiser avec les seuils Open DPE
    r2 = minimize(
        lambda s: score_with_penalty([nc2, ne2, *s], df, gap_min=0.30),
        x0=[2.00, 0.005, 0.010, 2.50, 0.003, 0.015],
        method="Nelder-Mead",
        options={"maxiter": 20000, "xatol": 1e-8, "fatol": 0.5},
    )
    opt2 = [nc2, ne2] + list(r2.x)
    pct2 = confusion(df, opt2, "Norm mean/std optimisée")
    print(f"  ({time()-t0:.1f}s)")

    # ══════════════════════════════════════════════════════════════════════
    # NORMALISATION 3 — Médiane / IQR (robuste)
    # ══════════════════════════════════════════════════════════════════════
    nc3 = float(cn.median())
    ne3 = float(cn.quantile(0.75) - cn.quantile(0.25))
    print(f"\n{'='*60}")
    print(f"  NORM 3 — median/IQR (robuste)")
    print(f"  centre={nc3:.0f}  échelle={ne3:.0f}")
    print(f"{'='*60}")

    t0 = time()
    r3 = minimize(
        lambda s: score_with_penalty([nc3, ne3, *s], df, gap_min=0.30),
        x0=[2.00, 0.005, 0.010, 2.50, 0.003, 0.015],
        method="Nelder-Mead",
        options={"maxiter": 20000, "xatol": 1e-8, "fatol": 0.5},
    )
    opt3 = [nc3, ne3] + list(r3.x)
    pct3 = confusion(df, opt3, "Norm median/IQR optimisée")
    print(f"  ({time()-t0:.1f}s)")

    # ══════════════════════════════════════════════════════════════════════
    # NORMALISATION 4 — Optimisation complète (nc + ne + 6 seuils)
    # Differential Evolution (global) pour éviter les minima locaux
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n{'='*60}")
    print(f"  NORM 4 — Optimisation globale (8 params, Diff. Evolution)")
    print(f"{'='*60}")

    t0 = time()
    bounds = [
        (35000, 55000),   # nc : centre
        (5000, 16000),    # ne : échelle
        (1.0, 5.0),       # sbr_intercept
        (0.001, 0.05),    # sbr_coeff_jour
        (0.001, 0.10),    # sbr_coeff_stock
        (1.5, 6.0),       # sr_intercept
        (0.001, 0.05),    # sr_coeff_jour
        (0.001, 0.10),    # sr_coeff_stock
    ]
    r4 = differential_evolution(
        lambda p: score_with_penalty(p, df, gap_min=0.35),
        bounds=bounds,
        maxiter=500,
        seed=42,
        tol=0.5,
        polish=True,
    )
    opt4 = list(r4.x)
    pct4 = confusion(df, opt4, "Optimisation globale (8 params)")
    print(f"  ({time()-t0:.1f}s)")

    # ══════════════════════════════════════════════════════════════════════
    # RÉSUMÉ
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n\n{'='*60}")
    print(f"  RÉSUMÉ COMPARATIF")
    print(f"{'='*60}")
    results = [
        ("1. q40/q80 (actuel)",    opt1, pct1),
        ("2. mean/std",            opt2, pct2),
        ("3. median/IQR",          opt3, pct3),
        ("4. Global (DE)",         opt4, pct4),
    ]
    best_pct = max(r[2] for r in results)
    names = ["norm_centre","norm_echelle","sbr_i","sbr_j","sbr_s","sr_i","sr_j","sr_s"]
    for label, params, pct in results:
        _, seuil_br, seuil_r = apply_algorithm(df, params)
        gap = (seuil_r - seuil_br).mean()
        mk = "  ◄" if abs(pct - best_pct) < 0.01 else ""
        print(f"  {label:<30s}  {pct:.1f}%  gap={gap:.3f}  "
              f"nc={params[0]:.0f}  ne={params[1]:.0f}{mk}")

    # Meilleur résultat
    best = max(results, key=lambda r: r[2])
    print(f"\n  ◄ Meilleur : {best[0]} — {best[2]:.1f}%")
    print(f"\n  Paramètres :")
    for n, v in zip(names, best[1]):
        print(f"    {n:>20s} = {v:.6f}")

    # Écriture en base
    print(f"\n  → Injection dans gold.tempo_params... ", end="")
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO gold.tempo_params
            (norm_centre, norm_echelle, sbr_intercept, sbr_coeff_jour, sbr_coeff_stock,
             sr_intercept, sr_coeff_jour, sr_coeff_stock, pct_match, total_jours)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (*[float(x) for x in best[1]], float(best[2]), len(df)))
    conn.commit()
    cur.close()
    conn.close()
    print(f"OK → gold.tempo_params")

if __name__ == "__main__":
    main()
