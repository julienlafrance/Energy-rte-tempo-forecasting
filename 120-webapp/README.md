# Energy Monitoring — Streamlit WebApp

Interface web pour la plateforme de monitoring énergétique. Se connecte à l'API FastAPI (`110-api`).

## Configuration

L'URL de l'API est configurable via la variable d'environnement `API_URL` (défaut : `http://localhost:8000`).

```bash
export API_URL=http://localhost:8000
```

Un fichier `.env.example` est fourni comme référence.

## Lancement (dev local)

```bash
cd 120-webapp
pip install streamlit requests plotly
API_URL=http://localhost:8000 streamlit run app.py
```

## Docker

```bash
docker build -t energy-webapp .
docker run -e API_URL=http://energy-api:8000 -p 8501:8501 energy-webapp
```

## Pages

| Page | Description |
|------|-------------|
| Consommation | Prévision de consommation Linky à 72h avec graphique Plotly et intervalle de confiance |

## Structure

```
120-webapp/
├── app.py              # Point d'entrée Streamlit
├── config.py           # Variable API_URL (env var)
├── api_client.py       # Client HTTP vers l'API FastAPI
├── views/
│   ├── __init__.py
│   └── consumption.py  # Vue prévision consommation
├── Dockerfile
└── .env.example
```
