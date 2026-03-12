# ------------------------------
# Dockerfile pour FastAPI MLOps
# ------------------------------

FROM python:3.11-slim

WORKDIR /app

# Installer dépendances système nécessaires pour psycopg2-binary
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copier requirements et installer Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copier le code de l'application (dans api/ pour préserver le nom du package)
COPY . ./api/

# Déclarer les variables d'environnement par défaut (peuvent être surchargées au runtime)
ENV PG_HOST=projet-db \
    PG_DB=airflow \
    PG_USER=airflow \
    PG_PASS=airflow \
    PYTHONPATH=/app

# Exposer le port FastAPI
EXPOSE 8000

# Lancer uvicorn
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
