This guide walks through how to set up, test, and run the pipeline from start to finish.

## Part 1: Local Setup

### 1.1 Prerequisites

- Python 3.9+
- GCP account with BigQuery enabled
- Service account key for BigQuery authentication
- Git, optional

### 1.2 Directory Structure

```text
Skip/
├── ingest.py
├── airflow_dag.py
├── README.md
├── execution_instructions.md
├── skip-comics-9b8c28f81a6b.json
└── dbt/
    ├── dbt_project.yml
    ├── profiles.yml
    └── models/
        ├── staging/
        │   └── staging.sql
        ├── marts/
        │   ├── dim_comic.sql
        │   ├── dim_date.sql
        │   └── fact_comic.sql
        └── schema.yml
```

## Part 2: GCP and BigQuery Setup

### 2.1 GCP Project

The current project ID used by `ingest.py` and dbt is:

```text
skip-comics
```

Enable the BigQuery API for this project.

### 2.2 Service Account

Create a service account with BigQuery permissions. The current dbt profile uses:

```yaml
method: service-account
project: skip-comics
dataset: xkcd_marts
location: northamerica-northeast2
```

Make sure the `keyfile` path in [dbt/profiles.yml](/Users/aadeshmehra/Desktop/Skip/dbt/profiles.yml:10) points to your downloaded service account JSON file.

### 2.3 BigQuery Datasets

The ingestion script writes raw data here:

```text
skip-comics.skipcomics.raw_comics
```

dbt writes transformed marts here:

```text
skip-comics.xkcd_marts
```

`ingest.py --mode setup` creates the raw dataset/table if they do not exist. dbt creates the marts tables when you run `dbt run`.

## Part 3: Python Ingestion

### 3.1 Install Dependencies

```bash
pip install requests google-cloud-bigquery
```

If you are using the existing virtual environment:

```bash
source venv/bin/activate
```

### 3.2 Current Ingestion Methods

The current callable methods in [ingest.py](/Users/aadeshmehra/Desktop/Skip/ingest.py:205) are:

- `setup()`: creates/checks the BigQuery dataset and raw table.
- `ingest_historical()`: fetches historical XKCD comics and inserts missing rows.
- `ingest_incremental()`: fetches only the latest comic if it is not already loaded.

The CLI uses:

```bash
venv/bin/python ingest.py --mode setup
venv/bin/python ingest.py --mode historical
venv/bin/python ingest.py --mode incremental
```

### 3.3 Configure Authentication

For the Python ingestion script, either set Application Default Credentials:

```bash
gcloud auth application-default login
```

or set the service account key explicitly:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account-key.json
```

### 3.4 Run Ingestion

Create/check the raw dataset and table:

```bash
venv/bin/python ingest.py --mode setup
```

Run the historical backfill:

```bash
venv/bin/python ingest.py --mode historical
```

Run the incremental latest-comic load:

```bash
venv/bin/python ingest.py --mode incremental
```

Verify raw rows in BigQuery:

```sql
SELECT COUNT(*) AS total_comics
FROM `skip-comics.skipcomics.raw_comics`;
```

## Part 4: Airflow

### 4.1 Install Airflow

```bash
pip install apache-airflow
```

### 4.2 Initialize Airflow

```bash
airflow db init
```

### 4.3 Create Airflow User

```bash
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

### 4.4 Copy DAG File

```bash
mkdir -p ~/airflow/dags
cp airflow_dag.py ~/airflow/dags/airflow_dag.py
```

### 4.5 Update DAG File

In [airflow_dag.py](/Users/aadeshmehra/Desktop/Skip/airflow_dag.py:13), make sure this path points to your local project:

```python
sys.path.insert(0, '/Users/aadeshmehra/Desktop/Skip')
```

The DAG imports these methods from `ingest.py`:

```python
ingest_historical
ingest_incremental
setup
get_bq_client
get_exisiting_comic_nums
fetch_comic
```

### 4.6 Test the DAG

```bash
airflow dags list
```

You should see:

```text
xkcd_ingestion
```

Test one run:

```bash
airflow dags test xkcd_ingestion 2026-01-01
```

Expected task IDs:

```text
poll_for_new_comic
ingest_latest_comic
ingestion_success
```

## Part 5: dbt

### 5.1 Install dbt

```bash
pip install dbt-bigquery
```

### 5.2 Current dbt Profile

The project uses:

```yaml
profile: xkcd
```

from [dbt/dbt_project.yml](/Users/aadeshmehra/Desktop/Skip/dbt/dbt_project.yml:5).

The repo-local profile is [dbt/profiles.yml](/Users/aadeshmehra/Desktop/Skip/dbt/profiles.yml:1), so run dbt with `--profiles-dir dbt` from the repo root.

### 5.3 Test dbt Connection

```bash
venv/bin/dbt debug --project-dir dbt --profiles-dir dbt
```

### 5.4 Compile dbt Models

```bash
venv/bin/dbt compile --project-dir dbt --profiles-dir dbt
```

### 5.5 Run dbt Models

```bash
venv/bin/dbt run --project-dir dbt --profiles-dir dbt
```

Expected models:

- `staging`: ephemeral model
- `dim_comic`: table in `xkcd_marts`
- `dim_date`: table in `xkcd_marts`
- `fact_comic`: table in `xkcd_marts`

### 5.6 Verify Marts in BigQuery

```sql
SELECT * FROM `skip-comics.xkcd_marts.dim_comic` LIMIT 5;
SELECT * FROM `skip-comics.xkcd_marts.dim_date` LIMIT 5;
SELECT * FROM `skip-comics.xkcd_marts.fact_comic` LIMIT 5;
```
