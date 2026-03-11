import os
#!/usr/bin/env python3
"""
Prévision couleur Tempo J+1 à J+7
-----------------------------------
Entrées (PostgreSQL Gold) :
  - gold.openmeteo_forecast   : prévisions météo 7 jours (9 features)
  - gold.tempo_params         : paramètres RTE optimisés
  - gold.tempo_analysis       : stock rouge/blanc actuel, dernier jour
Sortie :
  - gold.tempo_forecast       : probabilités BLUE/WHITE/RED par jour
Méthode :
  1. Elastic Net → prédiction conso nationale (MWh)
  2. Normalisation RTE → conso_nette_std
  3. Monte Carlo (50k tirages) → distribution vs seuils dynamiques
  4. Ajustement pression stock fin de saison
"""
import psycopg2
import numpy as np
from datetime import datetime
import sys
# ============================================================
# Coefficients Elastic Net (entraînés sur 178 jours)
# Features : temp_mean, temp_min, temp_max, precipitation_sum,
#            wind_speed_max, wind_gusts_max, humidity_mean,
#            cloudcover_mean, shortwave_radiation_sum
# ============================================================
MODEL = {
    "beta": np.array([
        -2104.52, -2508.69, -1917.57, -646.82,
        327.00, -110.34, 435.41, -225.55, -1293.83
    ]),
    "x_mean": np.array([9.52, 6.73, 12.57, 2.42, 16.60, 37.74, 84.18, 72.74, 6.18]),
    "x_std": np.array([4.94, 4.64, 5.55, 4.05, 5.60, 12.68, 7.18, 24.71, 4.31]),
    "y_mean": 53723.41,
    "std_resid": 5395.93,
}
SEASON_END = datetime(2026, 3, 31)
SEASON_DAYS = 212
TOTAL_RED = 22
TOTAL_WHITE = 43
N_SAMPLES = 50000
PG_CONFIG = dict(
    host=os.environ.get("PG_HOST", "projet-db"),
    database=os.environ.get("PG_DB", "airflow"),
    user=os.environ.get("PG_USER", "airflow"),
    password=os.environ.get("PG_PASS", "airflow"),
)
def get_rte_params(cur):
    """Récupère les derniers paramètres RTE optimisés."""
    cur.execute("""
        SELECT norm_centre, norm_echelle,
               sbr_intercept, sbr_coeff_jour, sbr_coeff_stock,
               sr_intercept, sr_coeff_jour, sr_coeff_stock
        FROM gold.tempo_params
        ORDER BY updated_at DESC LIMIT 1
    """)
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("Aucun paramètre dans gold.tempo_params")
    return {
        "nc": float(row[0]), "ne": float(row[1]),
        "sbr_i": float(row[2]), "sbr_j": float(row[3]), "sbr_s": float(row[4]),
        "sr_i": float(row[5]), "sr_j": float(row[6]), "sr_s": float(row[7]),
    }
def get_stock(cur):
    """Recupere le stock et le jour Tempo courants depuis raw.rte_tempo_calendar."""
    cur.execute("""
        SELECT
            22 - COUNT(*) FILTER (WHERE color = 'RED')   AS stock_r,
            43 - COUNT(*) FILTER (WHERE color = 'WHITE') AS stock_b,
            MAX(date) - DATE '2025-09-01' + 1            AS jour_tempo,
            MAX(date)                                    AS last_date
        FROM raw.rte_tempo_calendar
        WHERE date >= '2025-09-01'
          AND color IS NOT NULL
    """)
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("Aucune donnee dans raw.rte_tempo_calendar")
    return {
        "stock_r": int(row[0]),
        "stock_b": int(row[1]),
        "jour": int(row[2]),
        "last_date": row[3],
    }
def get_meteo_forecast(cur):
    """Lit les prévisions météo 7j depuis gold.openmeteo_forecast."""
    cur.execute("""
        SELECT date, temp_mean, temp_min, temp_max,
               precipitation_sum, wind_speed_max, wind_gusts_max,
               humidity_mean, cloudcover_mean, shortwave_radiation_sum
        FROM gold.openmeteo_forecast
        WHERE date >= CURRENT_DATE
        ORDER BY date
        LIMIT 7
    """)
    rows = cur.fetchall()
    if not rows:
        raise RuntimeError("Aucune prévision dans gold.openmeteo_forecast")
    return rows
def predict_conso(x_raw):
    """Prédit la conso nationale via Elastic Net."""
    x_scaled = (x_raw - MODEL["x_mean"]) / MODEL["x_std"]
    return float(x_scaled @ MODEL["beta"] + MODEL["y_mean"])
def compute_seuils(params, jour, stock_r, stock_b):
    """Calcule les seuils dynamiques blanc/rouge et rouge."""
    seuil_br = params["sbr_i"] - params["sbr_j"] * jour - params["sbr_s"] * (stock_r + stock_b)
    seuil_r = params["sr_i"] - params["sr_j"] * jour - params["sr_s"] * stock_r
    return float(seuil_br), float(seuil_r)
def monte_carlo(conso_pred, seuil_br, seuil_r, params, month, dow):
    """
    Monte Carlo : tire N échantillons de conso, normalise, compare aux seuils.
    Retourne (p_blue, p_white, p_red).
    """
    samples = np.random.normal(conso_pred, MODEL["std_resid"], N_SAMPLES)
    samples_std = (samples - params["nc"]) / params["ne"]
    is_winter_weekday = (month in [11, 12, 1, 2, 3]) and (dow not in [5, 6])
    is_not_sunday = dow != 6
    if is_winter_weekday:
        n_red = int(np.sum(samples_std > seuil_r))
        n_white = int(np.sum((samples_std > seuil_br) & (samples_std <= seuil_r)))
        n_blue = N_SAMPLES - n_red - n_white
    elif is_not_sunday:
        n_red = 0
        n_white = int(np.sum(samples_std > seuil_br))
        n_blue = N_SAMPLES - n_white
    else:
        n_red, n_white, n_blue = 0, 0, N_SAMPLES
    return n_blue / N_SAMPLES, n_white / N_SAMPLES, n_red / N_SAMPLES
def adjust_stock_pressure(p_blue, p_white, p_red, stock_r, stock_b, days_left, month, dow):
    """Ajuste les probas selon la pression du stock restant."""
    if days_left <= 0:
        return p_blue, p_white, p_red
    is_winter_weekday = (month in [11, 12, 1, 2, 3]) and (dow not in [5, 6])
    is_not_sunday = dow != 6
    pr_r = (stock_r / days_left) / (TOTAL_RED / SEASON_DAYS)
    pr_w = (stock_b / days_left) / (TOTAL_WHITE / SEASON_DAYS)
    if pr_r > 1.5 and is_winter_weekday:
        boost = min((pr_r - 1) * 0.20, 0.35)
        p_red = min(p_red + boost, 0.90)
    if pr_w > 1.5 and is_not_sunday:
        boost = min((pr_w - 1) * 0.15, 0.30)
        p_white = min(p_white + boost, 0.85)
    total = p_blue + p_white + p_red
    return p_blue / total, p_white / total, p_red / total
def ensure_table(cur):
    """Crée la table gold.tempo_forecast si nécessaire."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gold.tempo_forecast (
            date                  DATE PRIMARY KEY,
            temp_mean             DOUBLE PRECISION,
            conso_pred_mwh        DOUBLE PRECISION,
            conso_lower_mwh       DOUBLE PRECISION,
            conso_upper_mwh       DOUBLE PRECISION,
            conso_nette_std_pred  DOUBLE PRECISION,
            seuil_blanc_rouge     DOUBLE PRECISION,
            seuil_rouge           DOUBLE PRECISION,
            p_blue                DOUBLE PRECISION,
            p_white               DOUBLE PRECISION,
            p_red                 DOUBLE PRECISION,
            couleur_predite       TEXT,
            confiance             DOUBLE PRECISION,
            stock_rouge           INT,
            stock_blanc           INT,
            jour_tempo            INT,
            jours_restants        INT,
            updated_at            TIMESTAMPTZ DEFAULT NOW()
        );
    """)
def upsert_forecast(cur, row):
    """Insert/update une ligne de prévision."""
    cur.execute("""
        INSERT INTO gold.tempo_forecast
            (date, temp_mean, conso_pred_mwh, conso_lower_mwh, conso_upper_mwh,
             conso_nette_std_pred, seuil_blanc_rouge, seuil_rouge,
             p_blue, p_white, p_red, couleur_predite, confiance,
             stock_rouge, stock_blanc, jour_tempo, jours_restants, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, NOW())
        ON CONFLICT (date) DO UPDATE SET
            temp_mean=EXCLUDED.temp_mean, conso_pred_mwh=EXCLUDED.conso_pred_mwh,
            conso_lower_mwh=EXCLUDED.conso_lower_mwh, conso_upper_mwh=EXCLUDED.conso_upper_mwh,
            conso_nette_std_pred=EXCLUDED.conso_nette_std_pred,
            seuil_blanc_rouge=EXCLUDED.seuil_blanc_rouge, seuil_rouge=EXCLUDED.seuil_rouge,
            p_blue=EXCLUDED.p_blue, p_white=EXCLUDED.p_white, p_red=EXCLUDED.p_red,
            couleur_predite=EXCLUDED.couleur_predite, confiance=EXCLUDED.confiance,
            stock_rouge=EXCLUDED.stock_rouge, stock_blanc=EXCLUDED.stock_blanc,
            jour_tempo=EXCLUDED.jour_tempo, jours_restants=EXCLUDED.jours_restants,
            updated_at=NOW()
    """, row)
def main():
    np.random.seed(42)
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    # Charger les données
    params = get_rte_params(cur)
    stock = get_stock(cur)
    meteo = get_meteo_forecast(cur)
    ensure_table(cur)
    print(f"Params RTE : centre={params['nc']:.0f}  echelle={params['ne']:.0f}")
    print(f"Stock      : {stock['stock_r']}R  {stock['stock_b']}B  jour {stock['jour']}")
    print(f"Meteo      : {len(meteo)} jours de previsions")
    print()
    header = f"{'Date':>12s} {'T°C':>5s} {'Conso':>8s} {'IC 95%':>16s} {'Std':>6s} {'SBR':>6s} {'SR':>6s} {'BLUE':>6s} {'WHITE':>6s} {'RED':>6s}  {'Pred':>10s} {'Conf':>5s}"
    print(header)
    print("-" * len(header))
    for i, row in enumerate(meteo):
        date_val = row[0]
        features = [float(v) if v is not None else 0.0 for v in row[1:]]
        x_raw = np.array(features)
        d = datetime.combine(date_val, datetime.min.time())
        jour = stock["jour"] + (i + 1)
        days_left = (SEASON_END - d).days
        dow = d.weekday()
        month = d.month
        # Prédiction conso
        conso = predict_conso(x_raw)
        conso_lo = conso - 1.96 * MODEL["std_resid"]
        conso_hi = conso + 1.96 * MODEL["std_resid"]
        conso_std = (conso - params["nc"]) / params["ne"]
        # Seuils dynamiques
        seuil_br, seuil_r = compute_seuils(params, jour, stock["stock_r"], stock["stock_b"])
        # Monte Carlo
        p_b, p_w, p_r = monte_carlo(conso, seuil_br, seuil_r, params, month, dow)
        # Pression stock
        p_b, p_w, p_r = adjust_stock_pressure(
            p_b, p_w, p_r, stock["stock_r"], stock["stock_b"], days_left, month, dow
        )
        # Résultat
        probs = {"BLEU": p_b, "BLANC": p_w, "ROUGE": p_r}
        gap_seuils = seuil_r - seuil_br
        if p_b >= 0.50:
            couleur = "BLEU"
            confiance = p_b
        elif p_r == 0.0:
            couleur = "BLANC"
            confiance = p_w
        elif gap_seuils >= 0.26:
            couleur = "ROUGE" if p_r > p_w else "BLANC"
            confiance = probs[couleur]
        else:
            couleur = "BLANC/ROUGE"
            confiance = p_w + p_r
        temp = features[0]
        print(
            f"{date_val!s:>12s} {temp:4.1f}C {conso:7.0f}MW"
            f" {conso_lo:7.0f}-{conso_hi:6.0f}"
            f" {conso_std:5.2f} {seuil_br:5.3f} {seuil_r:5.3f}"
            f" {p_b:5.1%} {p_w:5.1%} {p_r:5.1%}"
            f"  {couleur:>10s} {confiance:4.1%}"
        )
        upsert_forecast(cur, (
            str(date_val), temp, float(conso), float(conso_lo), float(conso_hi),
            float(conso_std), seuil_br, seuil_r,
            float(p_b), float(p_w), float(p_r), couleur, float(confiance),
            stock["stock_r"], stock["stock_b"], jour, days_left,
        ))
    conn.commit()
    cur.close()
    conn.close()
    print(f"\n{len(meteo)} jours ecrits dans gold.tempo_forecast")
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERREUR: {e}", file=sys.stderr)
        sys.exit(1)
