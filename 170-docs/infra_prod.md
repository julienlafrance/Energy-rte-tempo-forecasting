# Production Infrastructure

> Helm charts deployed on K3s — Julien's home server.

---

## Overview

```
K3s cluster (VM PROD)
├── kestra         Workflow orchestrator   :30082
├── energy-api     FastAPI inference API   :30088
└── energy-webapp  Streamlit dashboard     :30085
```

All services are exposed as **NodePort** on the host network. No ingress controller is used.

---

## Helm Charts

### `kestra/`

Kestra standalone deployment (v1.3.2 chart, v1.0.41 Helm).

| Setting | Value |
|---------|-------|
| Image | `kestra-prod:latest` (local build, `pullPolicy: Never`) |
| Mode | Standalone (single replica, no DinD) |
| Queue / Repository | PostgreSQL |
| Storage | Local filesystem (`/app/storage`) |
| Auth | Basic auth enabled |

**Host volumes mounted into the container:**

| Mount | Host path | Purpose |
|-------|-----------|---------|
| `/app/storage` | `~/projet/50-docker/kestra/data` | Kestra internal storage |
| `/app/scripts` | `~/projet/100-scripts` | Utility scripts |
| `/app/scripts_mlops` | `~/projet/100-scripts_mlops` | ML training & forecast scripts |
| `/app/dbt/linky` | `~/projet/60-dbt/linky` | dbt project for Gold layer |

**Ports:**

| Port | NodePort | Protocol |
|------|----------|----------|
| 8080 (HTTP) | 30082 | TCP |
| 8081 (Management) | 30083 | TCP |

**Database:** External PostgreSQL at `192.168.80.127:5432/airflow` (schema `kestra`).

---

### `energy-api/`

FastAPI serving forecast predictions from Gold tables.

| Setting | Value |
|---------|-------|
| Image | `saraelmountasser/fastapi-mlops:latest` |
| Replicas | 1 |
| Port | 8000 → NodePort 30088 |
| Health check | `GET /health` |

**Environment (via ConfigMap):**

| Variable | Value |
|----------|-------|
| `PG_HOST` | `192.168.80.127` |
| `PG_DB` | `airflow` |
| `PG_USER` | `airflow` |
| `PG_PASS` | `airflow` |

Liveness and readiness probes configured on `/health`.

---

### `energy-webapi/`

Streamlit web application for interactive forecast visualization.

| Setting | Value |
|---------|-------|
| Image | `saraelmountasser/energy-webapp:latest` |
| Replicas | 1 |
| Port | 880 → NodePort 30085 (targetPort 8501) |
| Health check | `GET /_stcore/health` |

**Environment (via ConfigMap):**

| Variable | Value |
|----------|-------|
| `API_URL` | `http://energy-api:8000` |

The webapp calls the API via Kubernetes internal DNS — no external network hop.

---

## MLOps Data Flow on the VM

```
MQTT broker ──► Kestra ──► PostgreSQL (Bronze/Silver)
                  │
                  ├──► dbt (Gold: linky_hourly)
                  │
                  ├──► mlops_train_linky_705.py ──► MLflow + S3
                  │
                  └──► mlops_forecast_linky_705.py ──► gold.mlops_linky_forecast
                                                            │
                                                   energy-api (FastAPI)
                                                            │
                                                   energy-webapp (Streamlit)
```

Scripts run inside the Kestra container via host-mounted volumes — no Docker-in-Docker required.

---

## Network Map

| Service | Internal port | NodePort | Access |
|---------|--------------|----------|--------|
| Kestra UI | 8080 | 30082 | `http://<VM_IP>:30082` |
| Kestra mgmt | 8081 | 30083 | `http://<VM_IP>:30083` |
| FastAPI | 8000 | 30088 | `http://<VM_IP>:30088` |
| Streamlit | 8501 | 30085 | `http://<VM_IP>:30085` |
| PostgreSQL | 5432 | — | `192.168.80.127:5432` |

---

## Deployment

Charts are installed with Helm on K3s:

```bash
helm upgrade --install kestra ./infra-prod/kestra -f infra-prod/kestra-values.yaml
helm upgrade --install energy-api ./infra-prod/energy-api
helm upgrade --install energy-webapp ./infra-prod/energy-webapi
```

Kestra flows are deployed separately via the CD pipeline (see [`170-docs/ci_cd.md`](ci_cd.md)).
