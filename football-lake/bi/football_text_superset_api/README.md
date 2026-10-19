# Football Text Intelligence 360 — Superset REST API provisioning

Create and validate **one dashboard** programmatically; no manual Superset UI chart building.

## Sections (19 charts)

A. **Model & Data Observability** (6): registered models, model version and MLflow run, document count, sentiment confidence, source coverage, inference batch/model lineage.
B. **Opinion** (4): daily volume, sentiment classes, sentiment balance, negative rates per source.
C. **Topic & Trends** (5): biggest topics, topic volume timeseries, 7-day trending, negative-topic alerts, anomalies.
D. **Scope & Entities** (4): domestic/international trends, mentions, entity trends, recent stories.

The dashboard uses 11 physical PostgreSQL tables, including `football_text_ai.text_mlflow_models` synced from MLflow. Other tables are written by the existing football_text_pipeline.

## Setup

```
pip install -r requirements.txt
cp .env.example .env
```

Edit `.env` (Superset host, administrator token/user, Superset `database id`, DWH schema, MLflow and Postgres). Avoid committing this file.

> DWH publication via your text inference pipeline must happen **before** provisioning the dashboard.  Model registry metadata is synchronized separately and read-only from MLflow.

```
python sync_mlflow_metadata.py
python create_text_dashboard.py --dry-run
python create_text_dashboard.py
python verify_dashboard.py <dashboard_id> --query-data
```

If not all inference tables are yet published: `python create_text_dashboard.py --skip-missing` (the resulting dashboard will have fewer charts).

The API script creates physical Superset datasets and charts when absent; matching chart names and dashboard title are updated rather than duplicated. It does **not** delete existing objects, but it **does replace the layout** of its own named dashboard, so do not manually modify that dashboard without considering the next provision run. Use a dedicated Superset service account with permissions to read the connected DB, create datasets/charts/dashboards, and access CSRF endpoints.

Common Superset API endpoints are:
- `POST /api/v1/security/login`
- `GET /api/v1/security/csrf_token/`
- `POST /api/v1/dataset/`
- `POST/PUT /api/v1/chart/`
- `POST/PUT /api/v1/dashboard/`
- `GET /api/v1/dashboard/<id>/charts`
- `GET /api/v1/chart/<id>/data/`

Superset visualization parameter layouts vary across versions. **The script is an API provisioning starter and has not been verified against your running Superset instance**. Validate with `verify_dashboard.py`, inspect the response errors, and adapt the chart configs for your particular version. In particular the legacy `dist_bar` chart may require an alternate `echarts_timeseries_bar` config on some installations.

## Data quality / model stats caveats

- `text_mlflow_models` reports model **Registry alias** and MLflow run metrics. It is **not** current live inference health and cannot substitute for batch telemetry.
- Prediction counts are the current DWH snapshot, not all-time cumulative totals; existing pipeline uses full refresh.
- `AVG(sentiment_confidence)` is not a calibrated accuracy measure. Weak-label sentiment should be treated as experimental until evaluated with human-labeled data.
- Domestic / international uses baseline rule-based scope and needs evaluation.
- Entity aliases are rule-based and incomplete.
- Dashboard scope: datasets may store model aliases and `model_uri` as strings, but **alias values can be retargeted**. To audit exact inference versions, log resolved model version/run ID in pipeline at prediction time.
- This release doesn't install Superset native filters; API native-filter schema is version-sensitive. Charts show their source data at current DWH snapshot and can be extended with scoped filters.

## API validation

The chart and dashboard REST APIs are documented at https://superset.apache.org/docs/rest-api/. For local server OpenAPI reference use `/swagger/v1` or `/api/v1/_openapi` if enabled for your version. Authentication and CSRF are handled by the client.
