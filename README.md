<p align="center">
  <img src="img/linky.png" alt="Linky" width="180"/>
</p>

<h1 align="center">⚡ Energy Forecasting — Projet 705</h1>
<p align="center"><em>DATA713 · MLOps · Télécom Paris · 2025-2026</em></p>

<p align="center">
  <b>EL MOUNTASSER Sara</b> &nbsp;·&nbsp;
  <b>ELAMINE Mohammed</b> &nbsp;·&nbsp;
  <b>LAFRANCE Julien</b> &nbsp;·&nbsp;
  <b>MERNISSI ARIFI Yassine</b>
  <br/>
  <sub>Encadrants : M. Cadapeaud Antonin · M. Prillard Martin</sub>
</p>

---

## Résumé

Prévision de la consommation électrique d'une maison réelle à partir du compteur **Linky** de Julien.  
Le système collecte les données en **event-driven** (MQTT), les transforme selon une **architecture Medallion**, entraîne un modèle **SARIMA** et expose les prédictions via une **API + webapp**.

---

## Architecture des données

```
Linky → Module radio → Home Assistant → Kestra → PostgreSQL (TimeSeries)
```

| Couche | Granularité | Contenu |
|--------|------------|---------|
| 🥉 Bronze | 10 s | Données brutes — `time, power_h, energy_wh, meter_id` |
| 🥈 Silver | 10 s | Données nettoyées — `time, power_w, energy_wh, meter_id` |
| 🥇 Gold | 1 h | Données analytiques — `time, consumption_kwh, hour, day, week` |

---

## Modèle ML

**SARIMA(2,0,0)(2,1,0,24)** — entraîné sur la couche Gold.

| Flow Kestra | Fréquence | Rôle |
|-------------|-----------|------|
| `mlops_linky_forecast_3d` | toutes les 6 h | Prévision des 3 prochains jours (72 h) |
| `mlops_linky_train` | hebdomadaire | Ré-entraînement sur fenêtre glissante |

Le modèle est stocké au format `.pkl` dans un **S3 Garage** :
```
s3://705/mlops/linky-sarima-705/<YYYYMMDDHH>/model.pkl
```

---

## Observabilité — MLflow

À chaque inférence, les métriques suivantes sont trackées sur **72 h prédites vs 72 h réelles** :

`MAE` · `MSE` · `RMSE` · `MAPE` · `Coverage 80%`

**Data Drift** détecté par test de Kolmogorov-Smirnov sur les inputs Gold :
> Fenêtre `[t-42j ; t-21j]` vs `[t-21j ; t]`

Forecast terminé ou drift détecté → **notification Discord** automatique.

---

## API & WebApp

| Service | Stack | Endpoints clés |
|---------|-------|----------------|
| **API** | FastAPI | `GET /health` · `GET /forecast/consumption?date=YYYY-MM-DD` |
| **WebApp** | Streamlit | Visualisation interactive des prévisions horaires |

La réponse `/forecast/consumption` retourne pour chaque heure : `predicted`, `lower`, `upper`.

---

## CI / CD / CT

```
CI  → GitHub Actions : validation qualité des flows à chaque PR
CD  → Push sur prod  : déploiement automatique des flows sur la VM
CT  → Hebdomadaire   : ré-entraînement SARIMA sur nouvelles données
```

---

## Infrastructure

```
Home-server Julien
├── VM PROD  →  K3S + Helm
└── VM DEV   →  Docker Compose
```

Services déployés : **Kestra · PostgreSQL · MLflow · FastAPI · Streamlit · S3 Garage**

---

## Structure du dépôt

```
10-flows/          Flows Kestra (DAGs YAML)
100-scripts_mlops/ Scripts Python ML (train + forecast)
130-tests/         Tests des flows
170-docs/          Documentation CD
50-docker/         Docker Compose (API, Kestra, MLflow, Postgres, WebApp)
img/               Assets visuels
```
