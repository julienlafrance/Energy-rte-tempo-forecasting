# Infrastructure de production

> Charts Helm déployés sur K3s — serveur personnel de Julien.

---

## Vue d'ensemble

```
Cluster K3s (VM PROD)
├── kestra         Orchestrateur de workflows   :30082
├── energy-api     API d'inférence FastAPI      :30088
└── energy-webapp  Dashboard Streamlit          :30085
```

Tous les services sont exposés en **NodePort** sur le réseau de l'hôte. Aucun ingress controller n'est utilisé.

---

## Charts Helm

### `kestra/`

Déploiement Kestra standalone (chart v1.3.2, Helm v1.0.41).

| Paramètre | Valeur |
|-----------|--------|
| Image | `kestra-prod:latest` (build local, `pullPolicy: Never`) |
| Mode | Standalone (un seul réplica, sans DinD) |
| Queue / Repository | PostgreSQL |
| Stockage | Système de fichiers local (`/app/storage`) |
| Authentification | Basic auth activée |

**Volumes hôte montés dans le conteneur :**

| Montage | Chemin hôte | Usage |
|---------|-------------|-------|
| `/app/storage` | `~/projet/50-docker/kestra/data` | Stockage interne Kestra |
| `/app/scripts` | `~/projet/100-scripts` | Scripts utilitaires |
| `/app/scripts_mlops` | `~/projet/100-scripts_mlops` | Scripts ML (entraînement & prévision) |
| `/app/dbt/linky` | `~/projet/60-dbt/linky` | Projet dbt pour la couche Gold |

**Ports :**

| Port | NodePort | Protocole |
|------|----------|-----------|
| 8080 (HTTP) | 30082 | TCP |
| 8081 (Management) | 30083 | TCP |

**Base de données :** PostgreSQL externe à `192.168.80.127:5432/airflow` (schéma `kestra`).

---

### `energy-api/`

FastAPI servant les prévisions de consommation depuis les tables Gold.

| Paramètre | Valeur |
|-----------|--------|
| Image | `saraelmountasser/fastapi-mlops:latest` |
| Réplicas | 1 |
| Port | 8000 → NodePort 30088 |
| Health check | `GET /health` |

**Environnement (via ConfigMap) :**

| Variable | Valeur |
|----------|--------|
| `PG_HOST` | `192.168.80.127` |
| `PG_DB` | `airflow` |
| `PG_USER` | `airflow` |
| `PG_PASS` | `airflow` |

Sondes liveness et readiness configurées sur `/health`.

---

### `energy-webapi/`

Application web Streamlit pour la visualisation interactive des prévisions.

| Paramètre | Valeur |
|-----------|--------|
| Image | `saraelmountasser/energy-webapp:latest` |
| Réplicas | 1 |
| Port | 880 → NodePort 30085 (targetPort 8501) |
| Health check | `GET /_stcore/health` |

**Environnement (via ConfigMap) :**

| Variable | Valeur |
|----------|--------|
| `API_URL` | `http://energy-api:8000` |

La webapp appelle l'API via le DNS interne Kubernetes — aucun saut réseau externe.

---

## Flux de données MLOps sur la VM

```
Broker MQTT ──► Kestra ──► PostgreSQL (Bronze/Silver)
                  │
                  ├──► dbt (Gold : linky_hourly)
                  │
                  ├──► mlops_train_linky_705.py ──► MLflow + S3
                  │
                  └──► mlops_forecast_linky_705.py ──► gold.mlops_linky_forecast
                                                            │
                                                   energy-api (FastAPI)
                                                            │
                                                   energy-webapp (Streamlit)
```

Les scripts s'exécutent dans le conteneur Kestra via les volumes montés depuis l'hôte — pas de Docker-in-Docker.

---

## Carte réseau

| Service | Port interne | NodePort | Accès |
|---------|-------------|----------|-------|
| Kestra UI | 8080 | 30082 | `http://<VM_IP>:30082` |
| Kestra mgmt | 8081 | 30083 | `http://<VM_IP>:30083` |
| FastAPI | 8000 | 30088 | `http://<VM_IP>:30088` |
| Streamlit | 8501 | 30085 | `http://<VM_IP>:30085` |
| PostgreSQL | 5432 | — | `192.168.80.127:5432` |

---

## Déploiement

Les charts sont installés avec Helm sur K3s :

```bash
helm upgrade --install kestra ./infra-prod/kestra -f infra-prod/kestra-values.yaml
helm upgrade --install energy-api ./infra-prod/energy-api
helm upgrade --install energy-webapp ./infra-prod/energy-webapi
```

Les flows Kestra sont déployés séparément via le pipeline CD (voir [`170-docs/ci_cd.md`](ci_cd.md)).
