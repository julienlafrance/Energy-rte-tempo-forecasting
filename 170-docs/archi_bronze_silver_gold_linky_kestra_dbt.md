# Architecture Medallion avec Kestra et dbt

## Vue d'ensemble

Cette documentation décrit la mise en place d'un pipeline de données **Bronze → Silver → Gold** utilisant :
- **Kestra** : orchestration des flux
- **dbt** : transformation des données
- **PostgreSQL/TimescaleDB** : stockage
- **MQTT** : ingestion temps réel

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│     BRONZE      │     │     SILVER      │     │      GOLD       │
│                 │     │                 │     │                 │
│  linky_raw      │────▶│  linky_energy   │────▶│  linky_hourly   │
│  (données       │     │  (nettoyage +   │     │  (agrégats +    │
│   brutes)       │     │   mapping)      │     │   métriques)    │
└─────────────────┘     └─────────────────┘     └─────────────────┘
       │                        │                       │
   Kestra MQTT              dbt run                 dbt run
   Trigger temps réel      Schedule */15min        Flow Trigger
```

---

## 1. Prérequis

### Infrastructure Docker

```
~/projet/50-docker/
├── kestra/
│   ├── docker-compose.yml
│   ├── build/
│   │   └── Dockerfile
│   ├── data/
│   └── tmp/
└── airflow/          # PostgreSQL partagé
    └── docker-compose.yml
```

### Réseau Docker

Tous les services doivent être sur le même réseau Docker :

```bash
docker network create airflow_default
```

---

## 2. Image Kestra personnalisée avec dbt

### Dockerfile

```dockerfile
# ~/projet/50-docker/kestra/build/Dockerfile
FROM kestra/kestra:latest

USER root

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir dbt-postgres

USER kestra
```

### docker-compose.yml

```yaml
# ~/projet/50-docker/kestra/docker-compose.yml
services:
  kestra:
    build:
      context: ./build
      dockerfile: Dockerfile
    image: kestra-dbt:latest
    container_name: kestra
    user: "root"
    command: server standalone
    ports:
      - "8082:8080"
    volumes:
      - ./data:/app/storage
      - ./tmp:/tmp/kestra-wd
      - /var/run/docker.sock:/var/run/docker.sock
    environment:
      KESTRA_CONFIGURATION: |
        datasources:
          postgres:
            url: jdbc:postgresql://airflow-postgres-1:5432/airflow?currentSchema=kestra
            username: airflow
            password: airflow
        kestra:
          server:
            basic-auth:
              username: admin@example.com
              password: MyPassword123
          repository:
            type: postgres
          storage:
            type: local
            local:
              basePath: /app/storage
          queue:
            type: postgres
    networks:
      - airflow_default

networks:
  airflow_default:
    external: true
```

### Construction et lancement

```bash
cd ~/projet/50-docker/kestra
docker compose build --no-cache
docker compose up -d
```

---

## 3. Structure des Namespace Files (dbt)

Les fichiers dbt sont stockés dans Kestra via les **Namespace Files**. Ils sont accessibles dans l'UI : **Files** (barre latérale gauche).

### Arborescence dans Kestra

```
namespace: projet705
└── _files/
    └── dbt/
        ├── dbt_project.yml
        ├── profiles.yml
        └── models/
            ├── sources.yml
            ├── silver/
            │   └── linky_energy.sql
            └── gold/
                └── linky_hourly.sql
```

### Fichiers de configuration dbt

#### dbt_project.yml

```yaml
name: 'linky'
version: '1.0.0'
config-version: 2

profile: 'linky'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:
  - "target"
  - "dbt_packages"

models:
  linky:
    silver:
      +schema: silver
      +materialized: incremental
    gold:
      +schema: gold
      +materialized: table
```

#### profiles.yml

```yaml
linky:
  outputs:
    dev:
      type: postgres
      host: airflow-postgres-1
      port: 5432
      user: airflow
      password: airflow
      dbname: airflow
      schema: dbt
      threads: 4
  target: dev
```

#### models/sources.yml

```yaml
version: 2

sources:
  - name: bronze
    schema: public
    tables:
      - name: linky_raw
        description: "Données brutes Linky depuis MQTT"
        columns:
          - name: ts
            description: "Timestamp de la mesure"
          - name: tier_1 
            description: "Index compteur tier 1 (Bleu HC)"
          - name: tier_2
            description: "Index compteur tier 2 (Bleu HP)"
          - name: tier_3
            description: "Index compteur tier 3 (Blanc HC)"
          - name: tier_4
            description: "Index compteur tier 4 (Blanc HP)"
          - name: tier_5
            description: "Index compteur tier 5 (Rouge HC)"
          - name: tier_6
            description: "Index compteur tier 6 (Rouge HP)"
```

---

## 4. Modèles dbt

### Silver : linky_energy.sql

Transformation des données brutes avec mapping des tarifs Tempo.

```sql
-- models/silver/linky_energy.sql
-- Silver: Dédoublonnage + mapping tarif Tempo
{{
    config(
        materialized='incremental',
        unique_key=['hour', 'tier_num']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'linky_raw') }}
    {% if is_incremental() %}
    WHERE ts > (SELECT COALESCE(MAX(ts_mesure), '1970-01-01') FROM {{ this }})
    {% endif %}
),

unpivoted AS (
    SELECT 
        ts,
        1 AS tier_num,
        'bleu' AS tarif_couleur,
        'HC' AS periode,
        tier_1 / 1000.0 AS energie_kwh
    FROM source WHERE tier_1 IS NOT NULL
    
    UNION ALL
    
    SELECT 
        ts,
        2 AS tier_num,
        'bleu' AS tarif_couleur,
        'HP' AS periode,
        tier_2 / 1000.0 AS energie_kwh
    FROM source WHERE tier_2 IS NOT NULL
    
    UNION ALL
    
    SELECT 
        ts,
        3 AS tier_num,
        'blanc' AS tarif_couleur,
        'HC' AS periode,
        tier_3 / 1000.0 AS energie_kwh
    FROM source WHERE tier_3 IS NOT NULL
    
    UNION ALL
    
    SELECT 
        ts,
        4 AS tier_num,
        'blanc' AS tarif_couleur,
        'HP' AS periode,
        tier_4 / 1000.0 AS energie_kwh
    FROM source WHERE tier_4 IS NOT NULL
    
    UNION ALL
    
    SELECT 
        ts,
        5 AS tier_num,
        'rouge' AS tarif_couleur,
        'HC' AS periode,
        tier_5 / 1000.0 AS energie_kwh
    FROM source WHERE tier_5 IS NOT NULL
    
    UNION ALL
    
    SELECT 
        ts,
        6 AS tier_num,
        'rouge' AS tarif_couleur,
        'HP' AS periode,
        tier_6 / 1000.0 AS energie_kwh
    FROM source WHERE tier_6 IS NOT NULL
),

deduplicated AS (
    SELECT DISTINCT ON (date_trunc('hour', ts), tier_num)
        date_trunc('hour', ts) AS hour,
        tier_num,
        tarif_couleur,
        periode,
        energie_kwh,
        ts AS ts_mesure
    FROM unpivoted
    ORDER BY date_trunc('hour', ts), tier_num, ts DESC
)

SELECT * FROM deduplicated
```

### Gold : linky_hourly.sql

Calcul de la consommation horaire par différence entre mesures consécutives.

```sql
-- models/gold/linky_hourly.sql
-- Gold: Consommation horaire par tarif
SELECT
    hour,
    tarif_couleur,
    periode,
    tier_num,
    energie_kwh - LAG(energie_kwh) OVER (PARTITION BY tier_num ORDER BY hour) AS consommation_kwh,
    energie_kwh AS cumul_kwh
FROM {{ ref('linky_energy') }}
ORDER BY hour DESC, tier_num
```

---

## 5. Flows Kestra

### Bronze : Ingestion MQTT temps réel

```yaml
id: mqtt_linky_ingest
namespace: projet705

tasks:
  - id: insert_linky
    type: io.kestra.plugin.jdbc.postgresql.Query
    url: jdbc:postgresql://airflow-postgres-1:5432/airflow
    username: airflow
    password: airflow
    sql: |
      INSERT INTO linky_raw (ts, tier_1, tier_2, tier_3, tier_4, tier_5, tier_6)
      VALUES (
        NOW(),
        {{ trigger.body.tier_1 ?? 'NULL' }},
        {{ trigger.body.tier_2 ?? 'NULL' }},
        {{ trigger.body.tier_3 ?? 'NULL' }},
        {{ trigger.body.tier_4 ?? 'NULL' }},
        {{ trigger.body.tier_5 ?? 'NULL' }},
        {{ trigger.body.tier_6 ?? 'NULL' }}
      )

triggers:
  - id: mqtt_trigger
    type: io.kestra.plugin.mqtt.RealtimeTrigger
    server: tcp://192.168.1.x:1883
    clientId: kestra_linky
    topic: linky/energy
    serdeType: JSON
```

### Silver : Transformation dbt (schedule)

```yaml
id: dbt_linky_silver
namespace: projet705

tasks:
  - id: dbt_silver
    type: io.kestra.plugin.scripts.shell.Script
    taskRunner:
      type: io.kestra.plugin.core.runner.Process
    script: |
      cd /app/storage/main/projet705/_files/dbt
      dbt run --profiles-dir . --select linky_energy 2>&1

triggers:
  - id: schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "*/15 * * * *"
```

### Gold : Agrégation dbt (flow trigger)

```yaml
id: dbt_linky_gold
namespace: projet705

tasks:
  - id: dbt_gold
    type: io.kestra.plugin.scripts.shell.Script
    taskRunner:
      type: io.kestra.plugin.core.runner.Process
    script: |
      cd /app/storage/main/projet705/_files/dbt
      dbt run --profiles-dir . --select linky_hourly 2>&1

triggers:
  - id: after_silver
    type: io.kestra.plugin.core.trigger.Flow
    conditions:
      - type: io.kestra.plugin.core.condition.ExecutionFlowCondition
        flowId: dbt_linky_silver
        namespace: projet705
      - type: io.kestra.plugin.core.condition.ExecutionStatusCondition
        in:
          - SUCCESS
```

---

## 6. Procédure de mise en place

### Étape 1 : Créer la table Bronze

```sql
CREATE TABLE linky_raw (
    ts TIMESTAMPTZ DEFAULT NOW(),
    tier_1 BIGINT,
    tier_2 BIGINT,
    tier_3 BIGINT,
    tier_4 BIGINT,
    tier_5 BIGINT,
    tier_6 BIGINT
);

-- Optionnel : conversion en hypertable TimescaleDB
SELECT create_hypertable('linky_raw', 'ts');
```

### Étape 2 : Uploader les fichiers dbt dans Kestra

1. Ouvrir Kestra UI : http://localhost:8082
2. Aller dans **Files** (barre latérale)
3. Sélectionner le namespace (ex: `projet705`)
4. Créer l'arborescence `dbt/models/silver/` et `dbt/models/gold/`
5. Uploader chaque fichier :
   - `dbt/dbt_project.yml`
   - `dbt/profiles.yml`
   - `dbt/models/sources.yml`
   - `dbt/models/silver/linky_energy.sql`
   - `dbt/models/gold/linky_hourly.sql`

### Étape 3 : Créer les flows

1. Aller dans **Flows**
2. Cliquer **Create**
3. Copier/coller chaque flow YAML
4. Sauvegarder

### Étape 4 : Tester

```bash
# Vérifier que dbt fonctionne
docker exec kestra dbt --version

# Tester la connexion
docker exec -w /app/storage/main/projet705/_files/dbt kestra dbt debug --profiles-dir .

# Lancer manuellement Silver
# (via UI : Flows > dbt_linky_silver > Execute)

# Vérifier les données
docker exec -i airflow-postgres-1 psql -U airflow -c "SELECT * FROM dbt_silver.linky_energy;"
docker exec -i airflow-postgres-1 psql -U airflow -c "SELECT * FROM dbt_gold.linky_hourly;"
```

---

## 7. Tables résultantes

### Bronze : linky_raw (public)

| Colonne | Type | Description |
|---------|------|-------------|
| ts | TIMESTAMPTZ | Timestamp mesure |
| tier_1 | BIGINT | Index Bleu HC (Wh) |
| tier_2 | BIGINT | Index Bleu HP (Wh) |
| tier_3 | BIGINT | Index Blanc HC (Wh) |
| tier_4 | BIGINT | Index Blanc HP (Wh) |
| tier_5 | BIGINT | Index Rouge HC (Wh) |
| tier_6 | BIGINT | Index Rouge HP (Wh) |

### Silver : linky_energy (dbt_silver)

| Colonne | Type | Description |
|---------|------|-------------|
| hour | TIMESTAMPTZ | Heure (tronquée) |
| tier_num | INT | Numéro de tier (1-6) |
| tarif_couleur | TEXT | bleu/blanc/rouge |
| periode | TEXT | HC/HP |
| energie_kwh | NUMERIC | Index en kWh |
| ts_mesure | TIMESTAMPTZ | Timestamp original |

### Gold : linky_hourly (dbt_gold)

| Colonne | Type | Description |
|---------|------|-------------|
| hour | TIMESTAMPTZ | Heure |
| tarif_couleur | TEXT | bleu/blanc/rouge |
| periode | TEXT | HC/HP |
| tier_num | INT | Numéro de tier |
| consommation_kwh | NUMERIC | Conso de l'heure |
| cumul_kwh | NUMERIC | Index cumulé |

---

## 8. Mapping Tarif Tempo

| Tier | Tarif | Période | Description |
|------|-------|---------|-------------|
| 1 | Bleu | HC | Heures creuses jour normal |
| 2 | Bleu | HP | Heures pleines jour normal |
| 3 | Blanc | HC | Heures creuses jour blanc |
| 4 | Blanc | HP | Heures pleines jour blanc |
| 5 | Rouge | HC | Heures creuses jour rouge |
| 6 | Rouge | HP | Heures pleines jour rouge |

---

## 9. Dépannage

### Kestra ne démarre pas

```bash
# Nettoyer les tables corrompues
docker exec -i airflow-postgres-1 psql -U airflow -c "TRUNCATE kestra.service_instance;"
docker compose restart
```

### dbt ne trouve pas les fichiers

```bash
# Vérifier les namespace files
docker exec kestra ls -la /app/storage/main/projet705/_files/dbt/
```

### Erreur de connexion PostgreSQL

```bash
# Tester depuis le conteneur Kestra
docker exec kestra dbt debug --profiles-dir /app/storage/main/projet705/_files/dbt
```

### Flow sans logs

Ajouter `2>&1` à la fin des commandes dbt pour capturer stderr.

---

## 10. Points clés

1. **Namespace Files** : Les fichiers dbt sont stockés dans Kestra, pas sur l'hôte
2. **Process TaskRunner** : dbt s'exécute directement dans le conteneur Kestra (pas Docker-in-Docker)
3. **Image custom** : Kestra + dbt + git dans une seule image
4. **Flow Trigger** : Gold se déclenche automatiquement après Silver SUCCESS
5. **Incremental** : Silver ne traite que les nouvelles données
