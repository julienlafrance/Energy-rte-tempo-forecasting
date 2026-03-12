import math
import psycopg2
from datetime import datetime, timedelta
from api.db import get_connection


def _safe_float(val):
    """Remplace NaN/Infinity par None pour la sérialisation JSON."""
    if val is None or not isinstance(val, (int, float)):
        return val
    return val if math.isfinite(val) else None

# Au plus proche de la donnée
def fetch_consumption_forecast(target_date):
    """
    Lit les 24 heures de prévision pour la date demandée depuis gold.linky_forecast.
    """
    conn = get_connection()
    cur = conn.cursor()

    start_ts = datetime.combine(target_date, datetime.min.time())
    end_ts = start_ts + timedelta(hours=23, minutes=59, seconds=59)

    cur.execute("""
        SELECT hour, conso_kwh_predicted, conso_kwh_lower, conso_kwh_upper
        FROM gold.mlops_linky_forecast
        WHERE hour BETWEEN %s AND %s
        ORDER BY hour
    """, (start_ts, end_ts))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        return None

    # transforme en liste de dictionnaires
    predictions = [
        {
            "hour": row[0],
            "predicted": _safe_float(row[1]),
            "lower": _safe_float(row[2]),
            "upper": _safe_float(row[3])
        }
        for row in rows
    ]
    return predictions
