#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "psycopg2-binary",
#     "pandas",
#     "numpy",
#     "scikit-learn",
#     "xgboost",
#     "requests",
# ]
# ///
"""
benchmark_tempo.py  —  Benchmark 3 approches de prédiction couleur Tempo
=========================================================================
Approche 1 — BASELINE   : Elastic Net météo → Monte Carlo (pipeline actuel)
Approche 2 — RTE D-1    : Prévisions conso RTE publiées J-1 + éolien/solaire
Approche 3 — XGBOOST    : Classification directe BLEU/BLANC/ROUGE (GPU si dispo)

Usage :
  export PG_HOST=172.18.0.3 PG_DB=airflow PG_USER=airflow PG_PASS=airflow
  uv run python3 benchmark_tempo.py [--no-gpu] [--skip-rte]
"""

import os, sys, argparse, warnings
import requests, urllib3, psycopg2
import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
from sklearn.metrics import accuracy_score, confusion_matrix

warnings.filterwarnings("ignore")
urllib3.disable_warnings()

# ── Config ────────────────────────────────────────────────────────────────────
PG = dict(
    host    = os.environ.get("PG_HOST",  "projet-db"),
    database= os.environ.get("PG_DB",    "airflow"),
    user    = os.environ.get("PG_USER",  "airflow"),
    password= os.environ.get("PG_PASS",  "airflow"),
)
BASE_URL      = os.environ.get("BASE_URL",      "")
CLIENT_BASIC  = os.environ.get("CLIENT_BASIC",  "")
SEASON_START  = date(2025, 9, 1)

# ── Constantes Elastic Net figées dans tempo_forecast.py (178 jours) ─────────
MODEL_BETA  = np.array([-2104.52, -2508.69, -1917.57, -646.82,
                           327.00,  -110.34,   435.41, -225.55, -1293.83])
MODEL_XMEAN = np.array([9.52, 6.73, 12.57, 2.42, 16.60, 37.74, 84.18, 72.74, 6.18])
MODEL_XSTD  = np.array([4.94, 4.64,  5.55, 4.05,  5.60, 12.68,  7.18, 24.71,  4.31])
MODEL_YMEAN = 53723.41
MODEL_STDR  = 5395.93
N_MC        = 200_000

# ── GPU detection ─────────────────────────────────────────────────────────────
def detect_gpu():
    try:
        import cupy as cp
        cp.array([1.0])
        props = cp.cuda.runtime.getDeviceProperties(0)
        name  = props["name"].decode() if isinstance(props["name"], bytes) else props["name"]
        print(f"  GPU [cupy]  : {name}")
        return "cupy"
    except Exception:
        pass
    try:
        import torch
        if torch.cuda.is_available():
            print(f"  GPU [torch] : {torch.cuda.get_device_name(0)}")
            return "cuda"
    except Exception:
        pass
    print("  GPU         : non disponible — calculs sur CPU")
    return "cpu"

# ── Monte Carlo ───────────────────────────────────────────────────────────────
def monte_carlo(conso_pred, nc, ne, seuil_br, seuil_r, month, dow, backend="cpu"):
    xp = __import__("cupy") if backend == "cupy" else np
    samples     = xp.random.normal(conso_pred, MODEL_STDR, N_MC)
    samples_std = (samples - nc) / ne
    is_ww = (month in [11, 12, 1, 2, 3]) and (dow not in [5, 6])
    is_ns = (dow != 6)
    if is_ww:
        n_r = int(xp.sum(samples_std > seuil_r))
        n_w = int(xp.sum((samples_std > seuil_br) & (samples_std <= seuil_r)))
        n_b = N_MC - n_r - n_w
    elif is_ns:
        n_r = 0
        n_w = int(xp.sum(samples_std > seuil_br))
        n_b = N_MC - n_w
    else:
        n_r, n_w, n_b = 0, 0, N_MC
    return n_b / N_MC, n_w / N_MC, n_r / N_MC

# ══════════════════════════════════════════════════════════════════════════════
# CHARGEMENT DONNÉES
# ══════════════════════════════════════════════════════════════════════════════
def load_history(conn):
    """gold.tempo_analysis + raw.openmeteo_daily + silver.rte_energy"""
    df = pd.read_sql("""
        SELECT
            a.date,
            a.couleur_officielle,
            a.conso_nette_mwh,
            a.jour_tempo,
            a.stock_rouge_restant,
            a.stock_blanc_restant,
            f.temp_mean, f.temp_min, f.temp_max,
            f.precipitation_sum,
            f.wind_speed_max, f.wind_gusts_max,
            f.humidity_mean, f.cloudcover_mean,
            f.shortwave_radiation_sum,
            rte.wind_mw, rte.solar_mw
        FROM gold.tempo_analysis a
        LEFT JOIN raw.openmeteo_daily f
            ON f.date = a.date
        LEFT JOIN (
            SELECT
                DATE(ts_hour AT TIME ZONE 'Europe/Paris') AS date,
                SUM(CASE WHEN production_type = 'WIND'  THEN value ELSE 0 END) AS wind_mw,
                SUM(CASE WHEN production_type = 'SOLAR' THEN value ELSE 0 END) AS solar_mw
            FROM silver.rte_energy
            GROUP BY 1
        ) rte ON rte.date = a.date
        WHERE a.date >= '2025-09-01'
          AND a.couleur_officielle IS NOT NULL
        ORDER BY a.date
    """, conn, parse_dates=["date"])

    df["dow"]           = df["date"].dt.dayofweek
    df["month"]         = df["date"].dt.month
    df["label"]         = df["couleur_officielle"].map({"BLUE":"BLEU","WHITE":"BLANC","RED":"ROUGE"})
    num                 = df["couleur_officielle"].map({"BLUE":0,"WHITE":1,"RED":2})
    df["lag1"]          = num.shift(1)
    df["lag7"]          = num.shift(7)
    df["stock_r_ratio"] = df["stock_rouge_restant"] / 22.0
    df["stock_b_ratio"] = df["stock_blanc_restant"] / 43.0
    df["days_left"]     = (pd.Timestamp("2026-03-31") - df["date"]).dt.days
    df["wind_mw"]       = df["wind_mw"].fillna(0.0)
    df["solar_mw"]      = df["solar_mw"].fillna(0.0)

    n_meteo = df.dropna(subset=["temp_mean"]).shape[0]
    vc = df["couleur_officielle"].value_counts()
    print(f"\n  Historique  : {len(df)} jours  "
          f"({df['date'].min().date()} → {df['date'].max().date()})")
    print(f"  Météo dispo : {n_meteo}/{len(df)} jours")
    print(f"  Répartition : BLUE={vc.get('BLUE',0)}  "
          f"WHITE={vc.get('WHITE',0)}  RED={vc.get('RED',0)}")
    return df

def load_rte_params(conn):
    cur = conn.cursor()
    cur.execute("""SELECT norm_centre, norm_echelle,
                          sbr_intercept, sbr_coeff_jour, sbr_coeff_stock,
                          sr_intercept,  sr_coeff_jour,  sr_coeff_stock, updated_at
                   FROM gold.tempo_params ORDER BY updated_at DESC LIMIT 1""")
    r = cur.fetchone()
    if r is None:
        raise RuntimeError("gold.tempo_params vide")
    p = dict(nc=r[0], ne=r[1],
             sbr_i=r[2], sbr_j=r[3], sbr_s=r[4],
             sr_i=r[5],  sr_j=r[6],  sr_s=r[7])
    print(f"\n  Params RTE  : centre={p['nc']:.0f}  échelle={p['ne']:.0f}  (mis à jour {r[8].date()})")
    return p

# ══════════════════════════════════════════════════════════════════════════════
# CALENDRIER
# ══════════════════════════════════════════════════════════════════════════════
def fetch_holidays():
    holidays = set()
    for yr in [2025, 2026]:
        try:
            r = requests.get(
                f"https://calendrier.api.gouv.fr/jours-feries/metropole/{yr}.json",
                timeout=10)
            holidays.update(date.fromisoformat(d) for d in r.json().keys())
        except Exception as e:
            print(f"  ⚠ Jours fériés {yr} : {e}")
    print(f"  Jours fériés : {len(holidays)} chargés")
    return holidays

def build_school_vacances():
    """Vacances scolaires 2025-2026 hardcodées (arrêté du 7/12/2022).
    Retourne {date → set(zones)}, zones ∈ {"A","B","C"}.
    Zone C = Paris (Créteil, Montpellier, Paris, Toulouse, Versailles).
    """
    PERIODES = [
        (date(2025, 10, 18), date(2025, 11,  2), {"A", "B", "C"}),  # Toussaint
        (date(2025, 12, 20), date(2026,  1,  4), {"A", "B", "C"}),  # Noël
        (date(2026,  2,  7), date(2026,  2, 22), {"A"}),             # Hiver A
        (date(2026,  2, 14), date(2026,  3,  1), {"B"}),             # Hiver B
        (date(2026,  2, 21), date(2026,  3,  8), {"C"}),             # Hiver C (Paris)
        (date(2026,  4,  4), date(2026,  4, 19), {"A"}),             # Printemps A
        (date(2026,  4, 11), date(2026,  4, 26), {"B"}),             # Printemps B
        (date(2026,  4, 18), date(2026,  5,  3), {"C"}),             # Printemps C
    ]
    vac = {}
    for start, end, zones in PERIODES:
        d = start
        while d <= end:
            vac.setdefault(d, set()).update(zones)
            d += timedelta(days=1)
    n_days  = len(vac)
    n_all3  = sum(1 for z in vac.values() if len(z) == 3)
    n_paris = sum(1 for z in vac.values() if "C" in z)
    print(f"  Vacances scolaires : {n_days} jours, "
          f"{n_all3} jours 3 zones, {n_paris} jours avec Paris")
    return vac

# ══════════════════════════════════════════════════════════════════════════════
# API RTE — prévisions de consommation D-1
# ══════════════════════════════════════════════════════════════════════════════
def fetch_rte_forecasts_d1():
    """Récupère les prévisions D-1 de consommation via API RTE.
    Pagine par blocs de 28 jours (limite API).
    Retourne {date → moyenne MW journalière} pour D-1 ou REALISED.
    """
    if not BASE_URL:
        return {}
    try:
        # Token
        r = requests.post(f"{BASE_URL}/token/oauth/",
                          headers={"Authorization": f"Basic {CLIENT_BASIC}"},
                          verify=False, timeout=15)
        r.raise_for_status()
        token = r.json()["access_token"]

        result = {}
        # Paginer par blocs de 28 jours
        chunk_start = SEASON_START
        today = date.today()
        while chunk_start < today:
            chunk_end = min(chunk_start + timedelta(days=27), today)
            s = f"{chunk_start.strftime('%Y-%m-%d')}T00:00:00%2B01:00"
            e = f"{chunk_end.strftime('%Y-%m-%d')}T23:59:59%2B01:00"
            url = f"{BASE_URL}/open_api/consumption/v1/short_term?start_date={s}&end_date={e}"

            r2 = requests.get(url, headers={"Authorization": f"Bearer {token}"},
                              verify=False, timeout=60)
            if r2.status_code != 200:
                print(f"  ⚠ RTE API {r2.status_code} pour {chunk_start} → {chunk_end}")
                chunk_start = chunk_end + timedelta(days=1)
                continue

            for block in r2.json().get("short_term", []):
                btype = block.get("type", "")
                if btype not in ("D-1", "REALISED"):
                    continue
                for v in block.get("values", []):
                    try:
                        d = pd.Timestamp(v["start_date"]).date()
                        result.setdefault(d, {}).setdefault(btype, []).append(float(v["value"]))
                    except Exception:
                        pass

            print(f"    {chunk_start} → {chunk_end} : OK")
            chunk_start = chunk_end + timedelta(days=1)

        # Agréger : moyenne journalière, priorité D-1 > REALISED
        out = {}
        for d, data in result.items():
            vals = data.get("D-1") or data.get("REALISED", [])
            if vals:
                out[d] = np.mean(vals)
        print(f"  RTE D-1 prévisions : {len(out)} jours")
        return out
    except Exception as ex:
        print(f"  ⚠ API RTE : {ex}")
        return {}

# ══════════════════════════════════════════════════════════════════════════════
# APPROCHE 1 — Baseline Elastic Net + Monte Carlo
# ══════════════════════════════════════════════════════════════════════════════
def approach1(df, params, gpu):
    print("\n" + "═"*60)
    print("  APPROCHE 1 — Elastic Net + Monte Carlo (baseline)")
    print("═"*60)

    METEO_COLS = ["temp_mean","temp_min","temp_max","precipitation_sum",
                  "wind_speed_max","wind_gusts_max","humidity_mean",
                  "cloudcover_mean","shortwave_radiation_sum"]

    df_ok = df.dropna(subset=METEO_COLS).copy()
    print(f"  Jours avec météo : {len(df_ok)}/{len(df)}")
    preds, trues = [], []

    for _, row in df_ok.iterrows():
        x     = np.array([row[c] for c in METEO_COLS])
        conso = float((x - MODEL_XMEAN) / MODEL_XSTD @ MODEL_BETA + MODEL_YMEAN)

        j   = int(row["jour_tempo"])
        sr  = int(row["stock_rouge_restant"])
        sb  = int(row["stock_blanc_restant"])
        sbr = params["sbr_i"] - params["sbr_j"]*j - params["sbr_s"]*(sr+sb)
        srr = params["sr_i"]  - params["sr_j"]*j  - params["sr_s"]*sr

        pb, pw, pr = monte_carlo(conso, params["nc"], params["ne"],
                                  sbr, srr, row["month"], row["dow"], gpu)

        gap_seuils = srr - sbr
        if pb >= 0.50:
            couleur = "BLEU"
        elif pr == 0.0:
            couleur = "BLANC"
        elif gap_seuils >= 0.26:
            couleur = "ROUGE" if pr > pw else "BLANC"
        else:
            couleur = "BLANC"  # BLANC/ROUGE → BLANC conservateur

        preds.append(couleur)
        trues.append(row["label"])

    print_metrics(preds, trues)
    return preds, trues

# ══════════════════════════════════════════════════════════════════════════════
# APPROCHE 2 — Prévisions RTE D-1 + éolien/solaire
# ══════════════════════════════════════════════════════════════════════════════
def approach2(df, params, skip_api):
    print("\n" + "═"*60)
    print("  APPROCHE 2 — RTE Forecast D-1 + éolien/solaire (Open DPE)")
    print("═"*60)

    if skip_api:
        print("  Ignoré (--skip-rte)")
        return None, None

    rte_fc = fetch_rte_forecasts_d1()
    if not rte_fc:
        print("  ⚠ Pas de données — approche ignorée")
        return None, None

    cn   = df["conso_nette_mwh"].dropna()
    nc_c = float(cn.quantile(0.40))
    ne_c = float(cn.quantile(0.80) - cn.quantile(0.40))
    print(f"  Normalisation recalibrée : centre={nc_c:.0f}  échelle={ne_c:.0f}")

    preds, trues, skip = [], [], 0
    for _, row in df.iterrows():
        d = row["date"].date()
        if d not in rte_fc:
            skip += 1
            continue

        conso_nette = rte_fc[d] - row["wind_mw"] - row["solar_mw"]
        conso_std   = (conso_nette - nc_c) / ne_c

        j   = int(row["jour_tempo"])
        sr  = int(row["stock_rouge_restant"])
        sb  = int(row["stock_blanc_restant"])
        sbr = params["sbr_i"] - params["sbr_j"]*j - params["sbr_s"]*(sr+sb)
        srr = params["sr_i"]  - params["sr_j"]*j  - params["sr_s"]*sr

        dow, month = row["dow"], row["month"]
        ww = month in [11,12,1,2,3] and dow not in [5,6]
        ns = dow != 6

        if ww:
            c = "ROUGE" if conso_std > srr else ("BLANC" if conso_std > sbr else "BLEU")
        elif ns:
            c = "BLANC" if conso_std > sbr else "BLEU"
        else:
            c = "BLEU"

        preds.append(c)
        trues.append(row["label"])

    print(f"  Jours ignorés (sans prévision D-1) : {skip}")
    print_metrics(preds, trues)
    return preds, trues

# ══════════════════════════════════════════════════════════════════════════════
# APPROCHE 3 — XGBoost, features enrichies
# ══════════════════════════════════════════════════════════════════════════════
def approach3(df, gpu, holidays, school_vac):
    print("\n" + "═"*60)
    print("  APPROCHE 3 — XGBoost classification directe")
    print("═"*60)

    try:
        import xgboost as xgb
        print(f"  XGBoost : v{xgb.__version__}")
    except ImportError:
        print("  ⚠ pip install xgboost")
        return None, None, None

    from sklearn.model_selection import TimeSeriesSplit
    df = df.copy()

    # ── Features calendrier ───────────────────────────────────────────────
    df["is_ferie"]     = df["date"].dt.date.map(lambda d: int(d in holidays))
    df["vac_nb_zones"] = df["date"].dt.date.map(lambda d: len(school_vac.get(d, set())))
    df["vac_paris"]    = df["date"].dt.date.map(lambda d: int("C" in school_vac.get(d, set())))

    # ── Features météo enrichies ──────────────────────────────────────────
    df["temp_ma3"]  = df["temp_mean"].rolling(3, min_periods=1).mean()
    df["temp_lag1"] = df["temp_mean"].shift(1)

    # ── Conso nette lissée ────────────────────────────────────────────────
    df["conso_ma3"] = df["conso_nette_mwh"].rolling(3, min_periods=1).mean()

    # ── Pression stock / urgence ──────────────────────────────────────────
    days_safe = df["days_left"].clip(lower=1)
    df["urgence_rouge"] = df["stock_rouge_restant"] / days_safe
    df["urgence_blanc"] = df["stock_blanc_restant"] / days_safe
    df["urgence_total"] = (df["stock_rouge_restant"] + df["stock_blanc_restant"]) / days_safe

    FEATURES = [
        # Météo (9 + 2 enrichies)
        "temp_mean","temp_min","temp_max","temp_ma3","temp_lag1",
        "precipitation_sum",
        "wind_speed_max","wind_gusts_max",
        "humidity_mean","cloudcover_mean","shortwave_radiation_sum",
        # Calendaire
        "dow","month","jour_tempo","days_left",
        "is_ferie","vac_nb_zones","vac_paris",
        # Stock
        "stock_rouge_restant","stock_blanc_restant",
        "stock_r_ratio","stock_b_ratio",
        # Urgence (stock / jours restants)
        "urgence_rouge","urgence_blanc","urgence_total",
        # Lags couleur
        "lag1","lag7",
        # Renouvelable + conso
        "wind_mw","solar_mw","conso_ma3","conso_nette_mwh",
    ]

    df_ok = df.dropna(subset=FEATURES + ["couleur_officielle"]).copy()
    X = df_ok[FEATURES].values.astype(np.float32)
    y = df_ok["couleur_officielle"].map({"BLUE":0,"WHITE":1,"RED":2}).values

    print(f"  Features    : {len(FEATURES)}")
    print(f"  Samples     : {len(df_ok)}  (sur {len(df)} total)")
    cnt = dict(zip(*np.unique(y, return_counts=True)))
    print(f"  Répartition : BLEU={cnt.get(0,0)}  BLANC={cnt.get(1,0)}  ROUGE={cnt.get(2,0)}")

    device = "cuda" if gpu in ("cuda","cupy") else "cpu"
    print(f"  Device XGB  : {device}")

    PARAMS = dict(
        n_estimators     = 800,
        max_depth        = 4,
        learning_rate    = 0.03,
        subsample        = 0.8,
        colsample_bytree = 0.8,
        min_child_weight = 2,
        gamma            = 0.1,
        reg_alpha        = 0.5,
        reg_lambda       = 1.5,
        objective        = "multi:softmax",
        num_class        = 3,
        eval_metric      = "merror",
        device           = device,
        verbosity        = 0,
        random_state     = 42,
    )

    tscv = TimeSeriesSplit(n_splits=5)
    all_p, all_t, fold_accs = [], [], []

    # Garder dow/month pour les règles métier post-XGBoost
    dows   = df_ok["dow"].values
    months = df_ok["month"].values

    for fold, (tr, te) in enumerate(tscv.split(X), 1):
        X_tr, X_te = X[tr], X[te]
        y_tr, y_te = y[tr], y[te]

        counts = np.bincount(y_tr, minlength=3)
        w      = len(y_tr) / (3 * np.maximum(counts, 1))
        sw     = w[y_tr]

        clf = xgb.XGBClassifier(**PARAMS)
        clf.fit(X_tr, y_tr, sample_weight=sw,
                eval_set=[(X_te, y_te)], verbose=False)

        y_pred = clf.predict(X_te)

        # ── Règles métier Tempo (post-processing) ────────────────────
        # Dimanche (dow=6) → toujours BLEU
        # Samedi (dow=5) → jamais ROUGE → downgrade en BLANC
        # ROUGE interdit hors nov-mars en semaine
        for i, idx in enumerate(te):
            dow_i   = dows[idx]
            month_i = months[idx]
            if dow_i == 6:
                y_pred[i] = 0  # BLEU
            elif dow_i == 5 and y_pred[i] == 2:
                y_pred[i] = 1  # ROUGE → BLANC
            elif y_pred[i] == 2 and month_i not in [11, 12, 1, 2, 3]:
                y_pred[i] = 0  # ROUGE hors saison → BLEU

        acc = accuracy_score(y_te, y_pred)
        fold_accs.append(acc)
        all_p.extend(y_pred)
        all_t.extend(y_te)
        print(f"    Fold {fold}  —  train={len(tr):3d}  test={len(te):3d}  acc={acc:.1%}")

    print(f"\n  Score moyen CV : {np.mean(fold_accs):.1%} ± {np.std(fold_accs):.1%}")

    inv   = {0:"BLEU", 1:"BLANC", 2:"ROUGE"}
    preds = [inv[p] for p in all_p]
    trues = [inv[t] for t in all_t]
    print_metrics(preds, trues)

    # Feature importance sur modèle complet
    counts_all = np.bincount(y, minlength=3)
    sw_all     = (len(y) / (3 * np.maximum(counts_all, 1)))[y]
    clf_full   = xgb.XGBClassifier(**PARAMS)
    clf_full.fit(X, y, sample_weight=sw_all, verbose=False)

    imps = pd.Series(clf_full.feature_importances_, index=FEATURES)
    print("\n  Feature importance (top 12) :")
    for feat, imp in imps.nlargest(12).items():
        bar = "█" * max(1, int(imp * 80))
        print(f"    {feat:<30s} {imp:.4f}  {bar}")

    return preds, trues, clf_full

# ══════════════════════════════════════════════════════════════════════════════
# APPROCHE 4 — XGBoost régression conso → seuils + Monte Carlo
# ══════════════════════════════════════════════════════════════════════════════
def approach4(df, params, gpu, holidays, school_vac):
    print("\n" + "═"*60)
    print("  APPROCHE 4 — XGBoost régression conso → seuils")
    print("═"*60)

    try:
        import xgboost as xgb
    except ImportError:
        print("  ⚠ pip install xgboost")
        return None, None

    from sklearn.model_selection import TimeSeriesSplit
    from sklearn.metrics import mean_absolute_error
    df = df.copy()

    # ── Mêmes features calendrier/urgence que approach3 ───────────────────
    df["is_ferie"]     = df["date"].dt.date.map(lambda d: int(d in holidays))
    df["vac_nb_zones"] = df["date"].dt.date.map(lambda d: len(school_vac.get(d, set())))
    df["vac_paris"]    = df["date"].dt.date.map(lambda d: int("C" in school_vac.get(d, set())))
    df["temp_ma3"]     = df["temp_mean"].rolling(3, min_periods=1).mean()
    df["temp_lag1"]    = df["temp_mean"].shift(1)
    days_safe          = df["days_left"].clip(lower=1)
    df["urgence_rouge"]= df["stock_rouge_restant"] / days_safe
    df["urgence_blanc"]= df["stock_blanc_restant"] / days_safe

    # Features pour prédire la consommation nette (pas de conso en input!)
    FEAT_REG = [
        "temp_mean","temp_min","temp_max","temp_ma3","temp_lag1",
        "precipitation_sum",
        "wind_speed_max","wind_gusts_max",
        "humidity_mean","cloudcover_mean","shortwave_radiation_sum",
        "dow","month","jour_tempo","days_left",
        "is_ferie","vac_nb_zones","vac_paris",
        "wind_mw","solar_mw",
    ]

    df_ok = df.dropna(subset=FEAT_REG + ["conso_nette_mwh","couleur_officielle"]).copy()
    X = df_ok[FEAT_REG].values.astype(np.float32)
    y_conso = df_ok["conso_nette_mwh"].values.astype(np.float32)

    print(f"  Features    : {len(FEAT_REG)} (météo+calendrier → conso)")
    print(f"  Samples     : {len(df_ok)}")

    device = "cuda" if gpu in ("cuda","cupy") else "cpu"

    REG_PARAMS = dict(
        n_estimators     = 500,
        max_depth        = 4,
        learning_rate    = 0.05,
        subsample        = 0.8,
        colsample_bytree = 0.8,
        min_child_weight = 3,
        reg_alpha        = 0.5,
        reg_lambda       = 1.5,
        objective        = "reg:squarederror",
        device           = device,
        verbosity        = 0,
        random_state     = 42,
    )

    # Métadonnées pour seuils et règles métier
    jours   = df_ok["jour_tempo"].values
    sr_vals = df_ok["stock_rouge_restant"].values
    sb_vals = df_ok["stock_blanc_restant"].values
    dows    = df_ok["dow"].values
    months  = df_ok["month"].values
    labels  = df_ok["label"].values

    tscv = TimeSeriesSplit(n_splits=5)
    all_p, all_t, fold_accs, maes = [], [], [], []

    for fold, (tr, te) in enumerate(tscv.split(X), 1):
        X_tr, X_te = X[tr], X[te]
        y_tr, y_te = y_conso[tr], y_conso[te]

        reg = xgb.XGBRegressor(**REG_PARAMS)
        reg.fit(X_tr, y_tr, eval_set=[(X_te, y_te)], verbose=False)

        y_hat = reg.predict(X_te)
        mae   = mean_absolute_error(y_te, y_hat)
        maes.append(mae)

        # Incertitude = erreur sur le TEST set (pas train, car XGB overfitte le train)
        # On utilise RMSE test comme σ pour Monte Carlo — c'est l'erreur réelle
        resid_test = y_te - y_hat
        std_resid  = max(float(np.std(resid_test)), 1000.0)  # plancher 1000 MW

        # Appliquer seuils + Monte Carlo sur chaque prédiction
        fold_preds = []
        for i, idx in enumerate(te):
            conso_pred = float(y_hat[i])
            j   = int(jours[idx])
            sr  = int(sr_vals[idx])
            sb  = int(sb_vals[idx])
            sbr = params["sbr_i"] - params["sbr_j"]*j - params["sbr_s"]*(sr+sb)
            srr = params["sr_i"]  - params["sr_j"]*j  - params["sr_s"]*sr
            m   = int(months[idx])
            d   = int(dows[idx])

            # Monte Carlo avec résidu XGBoost (pas Elastic Net)
            xp = np
            samples     = xp.random.normal(conso_pred, std_resid, N_MC)
            samples_std = (samples - params["nc"]) / params["ne"]

            is_ww = (m in [11,12,1,2,3]) and (d not in [5,6])
            is_ns = (d != 6)
            if is_ww:
                n_r = int(xp.sum(samples_std > srr))
                n_w = int(xp.sum((samples_std > sbr) & (samples_std <= srr)))
                n_b = N_MC - n_r - n_w
            elif is_ns:
                n_r = 0
                n_w = int(xp.sum(samples_std > sbr))
                n_b = N_MC - n_w
            else:
                n_r, n_w, n_b = 0, 0, N_MC

            pb, pw, pr = n_b/N_MC, n_w/N_MC, n_r/N_MC

            gap = srr - sbr
            if pb >= 0.50:
                couleur = "BLEU"
            elif pr == 0.0:
                couleur = "BLANC"
            elif gap >= 0.26:
                couleur = "ROUGE" if pr > pw else "BLANC"
            else:
                couleur = "BLANC"

            fold_preds.append(couleur)

        fold_trues = [labels[idx] for idx in te]
        acc = accuracy_score(fold_trues, fold_preds)
        fold_accs.append(acc)
        all_p.extend(fold_preds)
        all_t.extend(fold_trues)
        print(f"    Fold {fold}  —  MAE={mae:.0f} MW  σ_resid={std_resid:.0f} MW  acc={acc:.1%}")

    print(f"\n  MAE moyen   : {np.mean(maes):.0f} MW  (vs Elastic Net σ={MODEL_STDR:.0f} MW)")
    print(f"  Score moyen : {np.mean(fold_accs):.1%} ± {np.std(fold_accs):.1%}")
    print_metrics(all_p, all_t)
    return all_p, all_t

# ══════════════════════════════════════════════════════════════════════════════
# MÉTRIQUES
# ══════════════════════════════════════════════════════════════════════════════
def print_metrics(preds, trues):
    if not preds:
        return
    labels = ["BLEU","BLANC","ROUGE"]
    acc    = accuracy_score(trues, preds)
    n_ok   = sum(p == t for p, t in zip(preds, trues))
    cm     = confusion_matrix(trues, preds, labels=labels)

    print(f"\n  Concordance : {acc:.1%}  ({n_ok}/{len(preds)})")
    print(f"  {'':>14s} | {'Off.BLEU':>8s} | {'Off.BLANC':>9s} | {'Off.ROUGE':>9s}")
    print(f"  {'-'*54}")
    for i, lbl in enumerate(labels):
        row   = cm[i] if i < len(cm) else [0,0,0]
        total = sum(t == lbl for t in trues)
        rec   = row[i] / total if total else 0.0
        print(f"  {'Prédit '+lbl:>14s} | {row[0]:>8d} | {row[1]:>9d} | {row[2]:>9d}"
              f"   recall {lbl}: {rec:.0%}")

def summarize(results):
    print("\n\n" + "═"*60)
    print("  RÉSUMÉ COMPARATIF")
    print("═"*60)
    valid = {n: (p,t) for n,(p,t) in results.items() if p}
    best  = max((accuracy_score(t,p) for p,t in valid.values()), default=0)
    for name, (preds, trues) in results.items():
        if not preds:
            print(f"  {name:<44s}  ignoré")
            continue
        acc = accuracy_score(trues, preds)
        mk  = "  ◄ MEILLEUR" if abs(acc - best) < 1e-9 else ""
        print(f"  {name:<44s}  {acc:.1%}{mk}")
    print()
    print("  Note : scores approche 3 en cross-validation temporelle")
    print("  (les données futures ne contaminent pas l'entraînement)")

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-gpu",   action="store_true")
    ap.add_argument("--skip-rte", action="store_true")
    args = ap.parse_args()

    print("═"*60)
    print("  BENCHMARK — Prédiction couleurs Tempo EDF")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("═"*60)

    gpu = "cpu" if args.no_gpu else detect_gpu()

    print("\n  Connexion PostgreSQL...")
    conn   = psycopg2.connect(**PG)
    params = load_rte_params(conn)
    df     = load_history(conn)
    conn.close()

    print("\n  Calendrier...")
    hol = fetch_holidays()
    vac = build_school_vacances()

    results = {}

    p1, t1 = approach1(df, params, gpu)
    results["1. Elastic Net + Monte Carlo (actuel)"] = (p1, t1)

    p2, t2 = approach2(df, params, args.skip_rte)
    if p2:
        results["2. RTE Forecast D-1 (approche Open DPE)"] = (p2, t2)

    out3 = approach3(df, gpu, hol, vac)
    if out3 and out3[0]:
        results["3. XGBoost classif + conso réelle"] = (out3[0], out3[1])

    p4, t4 = approach4(df, params, gpu, hol, vac)
    if p4:
        results["4. XGBoost régression → seuils+MC"] = (p4, t4)

    summarize(results)

if __name__ == "__main__":
    main()
