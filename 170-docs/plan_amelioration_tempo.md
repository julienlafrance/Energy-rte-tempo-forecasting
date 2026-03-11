# Plan : Nettoyage infrastructure + Amélioration Tempo + MLflow

---

## Phase 0 — Nettoyage infrastructure

### Objectif
Renommer les références "airflow" → "projet" dans Docker, nettoyer les fichiers legacy, créer la structure du repo git.

### Corrections au script bootstrap

Le script fourni est bon dans l'ensemble mais nécessite 3 corrections :

#### Correction 1 — Nettoyer le compose postgres (supprimer Airflow)
Le fichier `50-docker/airflow/docker-compose.yaml` contient les services Airflow (webserver, scheduler, triggerer, init, cli) en plus de postgres. Airflow n'est plus utilisé (remplacé par Kestra). Après renommage en `50-docker/postgres/`, il faut **ne garder que le service postgres/TimescaleDB**.

Compose cible (`50-docker/postgres/docker-compose.yaml`) :
```yaml
services:
  postgres:
    image: timescale/timescaledb-ha:pg16-all
    container_name: projet-db
    shm_size: '256mb'
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
      TIMESCALEDB_TELEMETRY: "off"
    volumes:
      - ./data/postgres-ha:/home/postgres/pgdata
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      retries: 5
      start_period: 5s
    restart: always
    networks:
      - projet-net

networks:
  projet-net:
    external: true
```

Supprimé : x-airflow-common, airflow-webserver, airflow-scheduler, airflow-triggerer, airflow-init, airflow-cli, tous les volumes Airflow (dags, logs, config, plugins).

#### Correction 2 — Réseau Docker `projet-net`
Le script crée `docker network create projet-net` mais le compose postgres ne le rejoint pas. Sans ça, Kestra/Elastic ne peuvent pas joindre postgres.

**Action** : Ajouter `networks: projet-net (external: true)` au compose postgres (fait dans correction 1 ci-dessus).

Compose Kestra — le sed `s/airflow_default/projet-net/g` est correct, le fichier a déjà `external: true`.

Compose Elastic — le sed `s/airflow_default/projet-net/g` changera `name: airflow_default` → `name: projet-net`. L'alias local `airflow` reste mais ça fonctionne. Optionnel : renommer aussi l'alias pour la cohérence :
```yaml
networks:
  projet-net:
    external: true
    name: projet-net
```
Et dans les services remplacer `- airflow` par `- projet-net`.

#### Correction 3 — Kestra flows en base
Les flows Kestra stockés dans PostgreSQL (schema `kestra`) référencent `airflow-postgres-1` dans leurs scripts inline. Après le renommage du container, ces flows seront cassés.

**Action** : UPDATE SQL sur les flows Kestra après redémarrage :
```sql
UPDATE kestra.flows
SET value = REPLACE(value, 'airflow-postgres-1', 'projet-db')
WHERE value LIKE '%airflow-postgres-1%';
```
Vérifier ensuite dans l'UI Kestra que les flows sont fonctionnels.

### Dossiers Airflow à supprimer
Après le rename `50-docker/airflow` → `50-docker/postgres`, supprimer les sous-dossiers Airflow devenus inutiles :
- `50-docker/postgres/dags/`
- `50-docker/postgres/logs/`
- `50-docker/postgres/config/`
- `50-docker/postgres/plugins/`
- `50-docker/postgres/Dockerfile` (si c'était l'image custom Airflow)

### Vérification Phase 0
1. `docker ps` → 3 containers : `projet-db`, `kestra`, `elasticsearch`, `kibana`
2. `docker exec projet-db psql -U airflow -c "SELECT 1"` → OK
3. Kestra UI (:8082) → flows visibles, pas d'erreur
4. Kibana UI (:5601) → dashboards accessibles
5. Lancer un flow Kestra simple pour vérifier la connectivité DB

---

## Phase 1 — Météo multi-villes (priorité)

### Principe
Calendrier Tempo utilise les températures de 9 grandes villes pondérées par consommation. Paris seul ne représente pas la demande nationale de chauffage.

### Implémentation
Appeler l'API Open-Meteo Archive pour 11 villes représentatives sur la période 2025-09-01 → aujourd'hui :

| Ville | Lat | Lon | Poids |
|-------|-----|-----|-------|
| Paris | 48.86 | 2.35 | 0.20 |
| Lyon | 45.76 | 4.84 | 0.12 |
| Marseille | 43.30 | 5.37 | 0.10 |
| Lille | 50.63 | 3.06 | 0.10 |
| Strasbourg | 48.57 | 7.75 | 0.08 |
| Bordeaux | 44.84 | -0.58 | 0.08 |
| Nantes | 47.22 | -1.55 | 0.08 |
| Toulouse | 43.60 | 1.44 | 0.08 |
| Nice | 43.71 | 7.27 | 0.06 |
| Rennes | 48.11 | -1.68 | 0.05 |
| Clermont-Fd | 45.78 | 3.08 | 0.05 |

Variables récupérées par ville (Open-Meteo Archive API, daily) :
- `temperature_2m_mean`, `temperature_2m_min`, `temperature_2m_max`
- `windspeed_10m_max`, `windgusts_10m_max`
- `precipitation_sum`
- `surface_pressure_mean` (nouveau)
- `shortwave_radiation_sum`

Features agrégées :
- `temp_mean_national` = Σ(poids × temp_mean_ville)
- `temp_spread` = max(temp_mean_villes) - min(temp_mean_villes)
- Idem pour min, max, vent, précipitations, pression

Features dérivées :
- **Gradient thermique** : `delta_temp = temp_mean_J - temp_mean_J-1`
- **Degrés-jours de chauffage** : `max(0, 18 - temp_mean_national)`
- **Pression atmosphérique** : signal vague de froid
- **Flag vague de froid** : temp_mean < 2°C ET delta_temp < -3°C

### Fichier
`/srv/int-705/100-scripts/benchmark_tempo_v2.py` — nouveau fichier

### Stockage
Cache parquet dans `/srv/int-705/90-data/tempo/weather_multi_city.parquet`

---

## Phase 2 — Historique multi-saisons

### Sources
- Couleurs Tempo historiques : API RTE "Tempo Like Supply Contract" (saisons 2022-2025)
- Consommation nationale : RTE éCO₂mix
- Production par filière : idem (éolien, solaire → conso nette)
- Météo historique : Open-Meteo Archive API (multi-villes)

### Stockage
`/srv/int-705/90-data/tempo/tempo_history_multisaison.parquet`

---

## Phase 3 — Objectif asymétrique + décision optimisée

- Score coût-pondéré : `5 × red_missed + 2 × white_missed + 1 × blue_missed`
- Seuil RED abaissé : P(RED) > 15-20% suffit
- Grid search sur seuils de décision
- XGBoost : focal loss ou scale_pos_weight RED × 10

---

## Phase 4 — Ensemble multi-modèles

- Vote pondéré par couleur
- Ou stacking logistique
- Règle de sécurité : ≥ 2 approches disent ROUGE → ROUGE

---

## Architecture MLflow

### Expérience : `tempo_benchmark_705`

**Tags** par run :
- `approach`, `version`, `season`, `date_range`, `run_type`

**Params** :
- Modèle (type, hyperparamètres), normalisation (méthode, nc, ne), seuils (6 coefficients), MC (n_samples, std_resid), features (liste, nombre), CV (n_folds), decision_rule (seuils)

**Metrics** (20+) :
- `accuracy`
- `recall_blue`, `recall_white`, `recall_red`
- `precision_blue`, `precision_white`, `precision_red`
- `f1_blue`, `f1_white`, `f1_red`
- `red_missed`, `red_false_alarm`, `white_missed`, `white_false_alarm`
- `cost_weighted_score` (RED manqué ×5, WHITE ×2, BLUE ×1)
- `gap_mean`, `std_resid`, `mae_conso`, `rmse_conso`
- 9 cellules confusion matrix : `cm_XX_YY`

**Artifacts sur S3 Garage** :
- `model.pkl` — modèle sérialisé
- `training_data.parquet` — dataset exact
- `script.py` — code source de la run
- `confusion_matrix.csv`
- `predictions.csv` — date / officielle / prédite / probabilités
- `tuning_history.json` — trajectoire d'optimisation complète
- `feature_importance.csv` (XGBoost)
- `params.json` — dump complet

**Backfill historique** (3 runs à remonter) :
1. `v1_baseline` — paramètres RTE originaux
2. `v2_optimized_v1` — après optimize_tempo.py
3. `v2_optimized_v2` — après optimize-tempo-v2.py

---

## Stockage des données

Parquets dans `/srv/int-705/90-data/tempo/` — pas en BDD tant que non validé :
```
90-data/tempo/
  weather_multi_city.parquet
  weather_features.parquet
  tempo_history_multisaison.parquet
  training_dataset.parquet
```

---

## Fichiers

| Fichier | Action |
|---------|--------|
| `50-docker/postgres/docker-compose.yaml` | **Réécrire** — garder postgres seul + réseau projet-net |
| `50-docker/kestra/docker-compose.yml` | **sed** — airflow-postgres-1 → projet-db, airflow_default → projet-net |
| `50-docker/elastic/docker-compose.yaml` | **sed** — airflow_default → projet-net (+ renommer alias) |
| `100-scripts/*.py` (5 fichiers) | **sed** — airflow-postgres-1 → projet-db |
| `50-docker/kestra/build/backfill_rte.sh` | **sed** — airflow-postgres-1 → projet-db |
| Kestra flows en base (schema kestra) | **UPDATE SQL** — airflow-postgres-1 → projet-db |
| `100-scripts/benchmark_tempo_v2.py` | **Créer** — nouveau benchmark + MLflow |
| `90-data/tempo/` | **Créer** — parquets intermédiaires |
| `120-doc/plan_amelioration_tempo.md` | Déjà créé |

---

## Vérification globale

### Phase 0
1. `docker ps` → projet-db, kestra, elasticsearch, kibana tous UP
2. `docker exec projet-db psql -U airflow -c "SELECT 1"` → OK
3. Kestra UI → flows visibles, connectivité DB OK
4. Kibana → dashboards accessibles

### Phases 1-4
1. `benchmark_tempo_v2.py` s'exécute sans erreur
2. MLflow UI → runs avec tous les params/metrics/artifacts
3. Parquets présents dans `90-data/tempo/`
4. Objectifs : accuracy > 85%, recall_red > 80%, recall_blanc > 30%
