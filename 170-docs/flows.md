# Flows Kestra — Documentation

Tous les flows s'exécutent sous le namespace `projet713`.
Les fichiers sources sont dans `10-flows/prod/`.

---

## Pipeline de données

### `mqtt_linky_ingest`

**Déclencheur :** MQTT temps réel — `linky/sensor/+/state`

Point d'entrée du pipeline de données. Écoute le broker MQTT pour les relevés du capteur Linky publiés par Home Assistant.

À chaque message :

1. **insert_bronze** — insère la métrique brute et sa valeur dans `raw.linky` (couche Bronze)
2. **downstream** (parallèle) :
   - appelle `mqtt_linky_silver` pour traiter les données en Silver

Le topic MQTT est parsé pour extraire le nom de la métrique en supprimant le préfixe `linky/sensor/lixee_zlinky_tic_` et le suffixe `/state`.

Les identifiants PostgreSQL sont chargés depuis le KV store Kestra (`PG_JDBC`, `PG_USER`, `PG_PASS`).

---

### `mqtt_linky_silver`

**Déclencheur :** MQTT temps réel — `linky/sensor/+/state`

Reprend la même structure que le flow d'ingestion. Insère les données nettoyées dans `raw.linky` (couche Silver) et distribue vers les mêmes sous-flows en aval.

Les identifiants PostgreSQL sont chargés depuis le KV store Kestra.

---

### `mqtt_linky_gold`

**Déclencheur :** Planifié — `5 * * * *` (toutes les heures à la 5ᵉ minute)

Agrège les données Silver dans la couche Gold via **dbt** :

1. Exécute `dbt run --select linky_hourly` dans le runner Kestra
2. Produit les données de consommation horaire dans `dbt_gold.linky_hourly`

Cette table est la source pour l'entraînement et l'inférence ML.

---

## Pipeline ML

### `mlops_train_forecast`

**Déclencheur :** Planifié — `0 0 * * 0` (hebdomadaire, dimanche à minuit)

**Script :** `100-scripts_mlops/mlops_train_linky_705.py`

Entraîne un modèle SARIMA(2,0,0)(2,1,0,24) sur les 21 derniers jours de consommation horaire.

Étapes :

1. Récupère les données depuis `dbt_gold.linky_hourly`
2. Interpole les heures manquantes, écrête les valeurs aberrantes (IQR × 3)
3. Ajuste le modèle SARIMA
4. Sauvegarde l'artefact modèle dans **S3 Garage** (`s3://705/mlops/linky-sarima-705/<YYYYMMDDHH>/model.pkl`)
5. Enregistre les paramètres et métriques dans **MLflow** (AIC, BIC, moyenne/écart-type de consommation)
6. Enregistre le modèle dans le **MLflow Registry** (`mlops_linky_sarima_705`)

En cas de succès : notification Discord.
En cas d'échec : notification Discord avec le détail de l'erreur.

| Variable KV | Usage |
|-------------|-------|
| `PG_HOST`, `PG_DB`, `PG_USER`, `PG_PASS` | Connexion PostgreSQL |
| `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_REGION`, `S3_BUCKET`, `S3_ENDPOINT` | Stockage S3 du modèle |
| `DISCORD_WEBHOOK_URL` | Notifications |

---

### `mlops_linky_forecast_3d`

**Déclencheur :** Planifié — `0 */6 * * *` (toutes les 6 heures)

**Script :** `100-scripts_mlops/mlops_forecast_linky_705.py`

Génère une prévision de consommation sur 72 heures à partir du dernier modèle SARIMA entraîné.

Étapes :

1. **Évaluation de la prévision précédente** — compare la dernière prévision complète aux consommations réelles et stocke MAE, MSE, RMSE, MAPE et couverture 80 % dans `gold.mlops_linky_performance`
2. **Récupération des données récentes** — charge les 21 derniers jours depuis `dbt_gold.linky_hourly`, interpole les trous, écrête les valeurs aberrantes
3. **Détection de dérive des données** — test de Kolmogorov-Smirnov comparant la fenêtre actuelle de 21 jours à la précédente ; résultat stocké dans `gold.mlops_linky_drift`
4. **Chargement du modèle** — récupère le dernier modèle depuis le MLflow Registry (repli sur le dernier run d'entraînement)
5. **Prévision** — applique les paramètres SARIMA entraînés pour générer 72 heures de prédictions avec intervalles de confiance à 80 %
6. **Sauvegarde** — insère les prédictions dans `gold.mlops_linky_forecast` (upsert par heure)
7. **Tracking MLflow** — enregistre les métadonnées de prévision, métriques de performance et indicateurs de dérive

En cas de succès : notification Discord.
En cas d'échec : notification Discord avec le détail de l'erreur.

| Table PostgreSQL | Contenu |
|-----------------|---------|
| `gold.mlops_linky_forecast` | Prédictions horaires avec intervalles de confiance |
| `gold.mlops_linky_performance` | Métriques de performance glissantes par prévision |
| `gold.mlops_linky_drift` | Résultats de détection de dérive des données |

Utilise les mêmes variables KV que le flow d'entraînement.

---

## Graphe de dépendances des flows

```
                    Broker MQTT
                        │
            ┌───────────┴───────────┐
            ▼                       ▼
   mqtt_linky_ingest       mqtt_linky_silver
            │
            ├──► mqtt_linky_silver
            └──► elastic_linky_realtime
                                    │
                        ┌───────────┘
                        ▼
                 mqtt_linky_gold          (cron horaire)
                        │
                        ▼
              dbt_gold.linky_hourly
                   ┌────┴────┐
                   ▼         ▼
  mlops_train_forecast    mlops_linky_forecast_3d
     (hebdomadaire)          (toutes les 6h)
         │                       │
         ▼                       ▼
   MLflow + S3             PostgreSQL + MLflow
```

---

## Variables du KV store

Tous les secrets et chaînes de connexion sont stockés dans le KV store Kestra — aucun identifiant en dur dans les flows.

| Clé | Description |
|-----|-------------|
| `PG_HOST` | Hôte PostgreSQL |
| `PG_DB` | Base de données PostgreSQL |
| `PG_USER` | Nom d'utilisateur PostgreSQL |
| `PG_PASS` | Mot de passe PostgreSQL |
| `PG_JDBC` | Chaîne de connexion JDBC (utilisée par les plugin defaults) |
| `S3_ACCESS_KEY` | Clé d'accès S3 |
| `S3_SECRET_KEY` | Clé secrète S3 |
| `S3_REGION` | Région S3 |
| `S3_BUCKET` | Nom du bucket S3 |
| `S3_ENDPOINT` | URL de l'endpoint S3 |
| `DISCORD_WEBHOOK_URL` | Webhook Discord pour les notifications |
