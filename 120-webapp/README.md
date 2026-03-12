# Energy Monitoring — Streamlit WebApp

Interface web pour la plateforme de monitoring energetique. Se connecte a l'API FastAPI (`110-api`).

## Setup

```bash
cd 120-webapp/webapp
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuration

L'URL de l'API est configurable via la variable d'environnement `API_URL` (defaut : `http://localhost:8000`).

```bash
# Exemple
export API_URL=http://localhost:8000
```

Un fichier `.env.example` est fourni comme reference.

## Lancement

```bash
source .venv/bin/activate
streamlit run app.py
```

Ou avec une URL API custom :

```bash
API_URL=http://192.168.80.212:8000 streamlit run app.py
```

## Pages

| Page | Description |
|------|-------------|
| Consommation | Prevision de consommation Linky a 72h avec graphique et intervalle de confiance |

## Structure

```
webapp/
├── app.py              # Point d'entree Streamlit
├── config.py           # Variable API_URL (env var)
├── api_client.py       # Client HTTP vers l'API FastAPI
├── pages/
│   └── consumption.py  # Page prevision consommation
├── requirements.txt
├── .env.example
└── .venv/
```
