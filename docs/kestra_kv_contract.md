# Kestra KV Contract — Namespace `projet705`

This document defines the Kestra Key-Value pairs required by all flows
in the `projet705` namespace. These must be provisioned in the Kestra KV
store **before** deploying the flows.

## PostgreSQL

| Key        | Example value                                              | Used by                                      |
|------------|------------------------------------------------------------|----------------------------------------------|
| `PG_JDBC`  | `jdbc:postgresql://projet-db:5432/airflow`                 | `mqtt_linky_ingest`, `mqtt_linky_silver`     |
| `PG_USER`  | `airflow`                                                  | `mqtt_linky_ingest`, `mqtt_linky_silver`     |
| `PG_PASS`  | *(secret)*                                                 | `mqtt_linky_ingest`, `mqtt_linky_silver`     |
| `PG_HOST`  | `projet-db`                                                | `mlops_linky_forecast_3d` (env for Python)   |

> `PG_JDBC` is used by the Kestra JDBC plugin (`pluginDefaults`).
> `PG_HOST` is passed as an environment variable to the Python forecast script.

## MQTT

| Key             | Example value                  | Used by              |
|-----------------|--------------------------------|----------------------|
| `MQTT_SERVER`   | `tcp://mosquitto:1883`         | `mqtt_linky_ingest`  |
| `MQTT_USERNAME` | `kestra`                       | `mqtt_linky_ingest`  |
| `MQTT_PASSWORD` | *(secret)*                     | `mqtt_linky_ingest`  |

## MLflow / S3

| Key                      | Example value                        | Used by                    |
|--------------------------|--------------------------------------|----------------------------|
| `MLFLOW_TRACKING_URI`    | `http://mlflow:8050`                 | `mlops_linky_forecast_3d`  |
| `MLFLOW_S3_ENDPOINT_URL` | `https://s3fast.lafrance.io`         | `mlops_linky_forecast_3d`  |
| `S3_ACCESS_KEY`          | *(secret)*                           | `mlops_linky_forecast_3d`  |
| `S3_SECRET_KEY`          | *(secret)*                           | `mlops_linky_forecast_3d`  |

## Elasticsearch

| Key        | Example value             | Used by                    |
|------------|---------------------------|----------------------------|
| `ES_URL`   | `https://es:9200`         | `mlops_linky_forecast_3d`  |
| `ES_PASS`  | *(secret)*                | `mlops_linky_forecast_3d`  |

## In-Container Runtime Paths

These are **not** KV keys — they are the canonical paths expected inside the
Kestra container, established by the Docker volume mounts:

| Container path            | Host source (configured in `.env`) | Content                |
|---------------------------|------------------------------------|------------------------|
| `/opt/projet705/scripts`  | `${HOST_SCRIPTS_PATH}`             | Python scripts         |
| `/opt/projet705/sql`      | `${HOST_SQL_PATH}`                 | Versioned SQL files    |
| `/opt/projet705/dbt`      | `${HOST_DBT_PATH}`                 | dbt project            |

## Docker `.env` File

Infrastructure-level credentials and host-specific paths are externalized
into `50-docker/kestra/.env` (git-ignored). A committed `.env.example`
template documents all required variables:

| Variable              | Description                           |
|-----------------------|---------------------------------------|
| `PG_HOST`             | PostgreSQL hostname                   |
| `PG_PORT`             | PostgreSQL port                       |
| `PG_DB`               | PostgreSQL database name              |
| `PG_USER`             | PostgreSQL user                       |
| `PG_PASS`             | PostgreSQL password                   |
| `KESTRA_PORT`         | Host port for Kestra UI               |
| `KESTRA_ADMIN_USER`   | Kestra basic-auth username            |
| `KESTRA_ADMIN_PASS`   | Kestra basic-auth password            |
| `HOST_DBT_PATH`       | Host path to the dbt project          |
| `HOST_SCRIPTS_PATH`   | Host path to scripts directory        |
| `HOST_SQL_PATH`       | Host path to sql directory            |
| `S3_HOST_IP`          | IP for S3 extra_hosts entry           |

## Provisioning

To set a key in Kestra (API or UI):

```bash
# Example via Kestra CLI / API
curl -X PUT "http://kestra:8080/api/v1/namespaces/projet705/kv/PG_HOST" \
  -H "Content-Type: application/json" \
  -d '"projet-db"'
```

Or use the Kestra UI: **Namespaces → projet705 → KV Store**.
