# Kestra Flows — Documentation

All flows run under the namespace `projet705`.

---

## Data pipeline

### `mqtt_linky_ingest`

**Trigger:** MQTT realtime — `linky/sensor/+/state`

Entry point of the data pipeline. Listens to the MQTT broker for Linky sensor readings published by Home Assistant.

On each message:

1. **insert_bronze** — inserts the raw metric and value into `raw.linky` (Bronze layer)
2. **downstream** (parallel):
   - calls `mqtt_linky_silver` to process the data into Silver
   - calls `elastic_linky_realtime` to index the data in Elasticsearch for real-time dashboards

The MQTT topic is parsed to extract the metric name by stripping the `linky/sensor/lixee_zlinky_tic_` prefix and `/state` suffix.

PostgreSQL credentials are loaded from Kestra KV store (`PG_JDBC`, `PG_USER`, `PG_PASS`).

---

### `mqtt_linky_silver`

**Trigger:** MQTT realtime — `linky/sensor/+/state`

Mirrors the ingest flow structure. Inserts cleaned data into `raw.linky` (Silver layer) and fans out to the same downstream subflows.

PostgreSQL credentials are loaded from Kestra KV store.

---

### `mqtt_linky_gold`

**Trigger:** Schedule — `5 * * * *` (every hour at minute 5)

Aggregates Silver data into the Gold layer using **dbt**:

1. Runs `dbt run --select linky_hourly` inside the Kestra runner
2. Produces hourly consumption data in `dbt_gold.linky_hourly`

This table is the source for both ML training and inference.

---

## ML pipeline

### `mlops_train_forecast`

**Trigger:** Schedule — `0 0 * * 0` (weekly, Sunday midnight)

**Script:** `100-scripts_mlops/mlops_train_linky_705.py`

Trains a SARIMA(2,0,0)(2,1,0,24) model on the last 21 days of hourly consumption data.

Steps:

1. Fetches data from `dbt_gold.linky_hourly`
2. Interpolates missing hours, caps outliers (IQR × 3)
3. Fits the SARIMA model
4. Saves the model artifact to **S3 Garage** (`s3://705/mlops/linky-sarima-705/<YYYYMMDDHH>/model.pkl`)
5. Logs parameters and metrics to **MLflow** (AIC, BIC, mean/std consumption)
6. Registers the model in **MLflow Registry** (`mlops_linky_sarima_705`)

On success: Discord notification.
On failure: Discord notification with error details.

| KV Variable | Usage |
|-------------|-------|
| `PG_HOST`, `PG_DB`, `PG_USER`, `PG_PASS` | PostgreSQL connection |
| `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_REGION`, `S3_BUCKET`, `S3_ENDPOINT` | S3 model storage |
| `DISCORD_WEBHOOK_URL` | Notifications |

---

### `mlops_linky_forecast_3d`

**Trigger:** Schedule — `0 */6 * * *` (every 6 hours)

**Script:** `100-scripts_mlops/mlops_forecast_linky_705.py`

Generates a 72-hour consumption forecast using the latest trained SARIMA model.

Steps:

1. **Evaluate previous forecast** — compares the last completed forecast against actual consumption and stores MAE, MSE, RMSE, MAPE, and 80% interval coverage in `gold.mlops_linky_performance`
2. **Fetch recent data** — loads the last 21 days from `dbt_gold.linky_hourly`, interpolates gaps, caps outliers
3. **Data drift detection** — Kolmogorov-Smirnov test comparing the current 21-day window against the previous 21-day window; result stored in `gold.mlops_linky_drift`
4. **Load model** — retrieves the latest model from MLflow Registry (falls back to the latest training run)
5. **Forecast** — applies the trained SARIMA parameters to generate 72 hours of predictions with 80% confidence intervals
6. **Save** — inserts predictions into `gold.mlops_linky_forecast` (upsert by hour)
7. **MLflow tracking** — logs forecast metadata, performance metrics, and drift indicators

On success: Discord notification.
On failure: Discord notification with error details.

| PostgreSQL Table | Content |
|-----------------|---------|
| `gold.mlops_linky_forecast` | Hourly predictions with confidence intervals |
| `gold.mlops_linky_performance` | Rolling performance metrics per forecast |
| `gold.mlops_linky_drift` | Data drift detection results |

Uses the same KV variables as the training flow.

---

## Flow dependency graph

```
                    MQTT broker
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
                 mqtt_linky_gold          (hourly cron)
                        │
                        ▼
              dbt_gold.linky_hourly
                   ┌────┴────┐
                   ▼         ▼
  mlops_train_forecast    mlops_linky_forecast_3d
     (weekly)                (every 6h)
         │                       │
         ▼                       ▼
   MLflow + S3             PostgreSQL + MLflow
```

---

## KV store variables

All secrets and connection strings are stored in the Kestra KV store — no hardcoded credentials in flows.

| Key | Description |
|-----|-------------|
| `PG_HOST` | PostgreSQL host |
| `PG_DB` | PostgreSQL database |
| `PG_USER` | PostgreSQL username |
| `PG_PASS` | PostgreSQL password |
| `PG_JDBC` | JDBC connection string (used by plugin defaults) |
| `S3_ACCESS_KEY` | S3 access key |
| `S3_SECRET_KEY` | S3 secret key |
| `S3_REGION` | S3 region |
| `S3_BUCKET` | S3 bucket name |
| `S3_ENDPOINT` | S3 endpoint URL |
| `DISCORD_WEBHOOK_URL` | Discord webhook for notifications |
