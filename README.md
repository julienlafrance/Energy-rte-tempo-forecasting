<p align="center">
  <img src="img/linky.png" alt="Linky" width="250"/>
</p>

<h1 align="center">⚡ Soutenance projet — DATA713 MLOps</h1>

<p align="center">
  <b>EL MOUNTASSER Sara</b> &nbsp;·&nbsp;
  <b>ELAMINE Mohammed</b> &nbsp;·&nbsp;
  <b>LAFRANCE Julien</b> &nbsp;·&nbsp;
  <b>MERNISSI ARIFI Yassine</b>
  <br/>
  <sub>Encadrants : M. CADAPEAUD Antonin · M. PRILLARD Martin</sub>
</p>

---

## Nos données

Récupération event-driven de la consommation électrique de la maison de Julien :

```
Linky -> Module radio -> Home Assistant -> Kestra -> PostgreSQL
```

Les données sont stockées dans notre Data Warehouse PostgreSQL orienté time-series selon une architecture Medallion :

- **Bronze** (brut): `time, power_h, energy_wh, meter_id` (1 entrée toutes les 10 secondes)
- **Silver** (nettoyé): `time, power_w, energy_wh, meter_id` (1 entrée toutes les 10 secondes)
- **Gold** (analytique): `time, consumption_kwh, hour, day, week` (1 entrée par heure)

### Flows Kestra ingestion/transform

- PRD: http://kestra713.lafrance.io
- Chaîne de flows: `mqtt_linky_ingest -> mqtt_linky_silver -> mqtt_linky_gold`

---

## Stockage du modèle (S3 Garage)

Après chaque training, le modèle est stocké au format `.pickle` dans le S3 Garage de Julien :

```
s3://705/mlops/linky-sarima-705/<YYYYMMDDHH>/model.pkl
```

- Endpoint: http://s3fast.lafrance.io
- Bucket: `705`

Pour chaque utilisation, le modèle est récupéré directement depuis S3.

---

## MLflow — performance & drift

- Lien MLflow: http://192.168.80.212:8050/#/experiments
- Entraînement: compétition de plusieurs **candidats SARIMA** à chaque run
- Sélection du meilleur modèle: **AIC minimal** parmi les candidats entraînés avec succès
- Évaluation à chaque exécution sur la dernière prévision complète: **72h prédites vs 72h réelles**
- Métriques suivies: `MAE`, `MSE`, `RMSE`, `MAPE`, `Coverage 80%`

### Tracking training multi-SARIMA

À chaque exécution d'entraînement:

- Tous les candidats sont tracés dans MLflow (succès **et** échec)
- Pour chaque candidat: `order`, `seasonal_order`, `status`, `candidate_X_aic`, `candidate_X_bic`
- En cas d'échec: `status=failed`, message `candidate_X_error`, et `AIC/BIC = NaN`
- Le meilleur est tracé explicitement: `best_model_order`, `best_model_seasonal_order`, `best_model_name`
- Ce meilleur modèle est ensuite:
  - loggé en artifact MLflow (`statsmodels`)
  - enregistré dans le **MLflow Model Registry** (si activé)
  - exporté dans le S3 Garage (si activé)

### Data Drift (entrées)

Comparaison de distribution des inputs Gold (consommation) entre :

- `[t-42j ; t-21j]`
- `[t-21j ; t]`

Méthode: test de Kolmogorov-Smirnov (KS), pour détecter un changement de comportement des données.

Forecast done (ou Data Drift) -> message Discord.

---

## API — FastAPI

- Lien FastAPI: https://api713.lafrance.io/docs
- Objectif: récupérer les prévisions via requêtes HTTP

### Endpoints

- `GET /health` : vérifie si l’API est UP/opérationnelle
- `GET /forecast/consumption?date=YYYY-MM-DD`

Réponse JSON (horaire) :

- `hour`: heure correspondante
- `predicted`: consommation prédite
- `lower`: borne basse de l’intervalle
- `upper`: borne haute de l’intervalle

---

## WebApp — Streamlit

- Objectif: visualiser les prévisions
- Lien Streamlit: https://webapp713.lafrance.io

---

## CI / CD / CT

| Pipeline | Déclencheur | Action |
|----------|-------------|--------|
| **CI** | Push / PR sur `main` | Validation YAML des flows + tests pytest |
| **CD** | Push sur `prod` ou dispatch manuel | Déploiement des flows sur la VM via Kestra API |
| **CT** | Hebdomadaire (cron Kestra) | Ré-entraînement SARIMA sur nouvelles données |

| Workflow | Fichier |
|----------|---------|
| Validation | `.github/workflows/validate.yml` |
| Déploiement | `.github/workflows/deploy.yml` |

Le CD s'exécute sur un **runner self-hosted** installé sur la VM de production.

> 📖 Documentation détaillée : [`170-docs/ci_cd.md`](170-docs/ci_cd.md)

### Développement local — pre-commit & Makefile

```bash
make install      # installe les dépendances + active pre-commit
make check        # lance toutes les vérifications (flows + tests + pre-commit)
make fix          # corrige automatiquement le formatting
```

Les hooks pre-commit reproduisent les mêmes checks que la CI (validation flows, pytest, trailing whitespace, YAML syntax).

---

## Monitoring

- Dashboard API (logs monitoring: records, API, errors, etc.)
- Lien Kibana: https://kibana.lafrance.io/app/dashboards#/view/1d3cf053-9060-4798-96e8-6204baacaaba?_g=(filters:!(),refreshInterval:(pause:!t,value:60000),time:(from:now-15m,to:now))

---

## Notre infrastructure

### Environnement DEV — Docker Compose

- Host: Kestra (`:5601`), FastAPI (`:8000`), WebApp (`:8501`), PostgreSQL (`:5432`)
- S3 séparé: `s3fast.lafrance.io`
- IP: `192.168.80.212`

### Environnement PRD — K3S + Helm

- Host: Kestra (`:5601`), FastAPI (`:30800`), WebApp (`:30085`), PostgreSQL (`:5432`)
- S3 séparé: `s3fast.lafrance.io`
- IP: `192.168.80.127`

> Pour plus de détails, voir [`170-docs/infra_prod.md`](170-docs/infra_prod.md)

---

## Structure du dépôt

```
10-flows/          Flows Kestra (DAGs YAML)
100-scripts_mlops/ Scripts Python ML (train + forecast)
130-tests/         Tests des flows et déploiement
170-docs/          Documentation CI/CD
50-docker/         Docker Compose (API, Kestra, MLflow, Postgres, WebApp)
img/               Assets visuels
```

---

## Merci à vous
