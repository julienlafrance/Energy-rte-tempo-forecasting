import os
import psycopg2

# Connexion à la bdd
PG_CONFIG = dict(
    host=os.environ.get("PG_HOST", "projet-db"),
    database=os.environ.get("PG_DB", "airflow"),
    user=os.environ.get("PG_USER", "airflow"),
    password=os.environ.get("PG_PASS", "airflow"),
)

def get_connection():
    return psycopg2.connect(
        host=PG_CONFIG["host"],
        database=PG_CONFIG["database"],
        user=PG_CONFIG["user"],
        password=PG_CONFIG["password"]
    )