"""
XKCD ANALYTICS PIPELINE - COMPLETE EXECUTION GUIDE
This guide walks through how to set up, test, and run the pipeline
from start to finish.
PART 1: LOCAL SETUP & TESTING
1.1 Prerequisites
Python 3.9+
GCP account with BigQuery enabled
Service account key for authentication
Git (optional, for version control)

1.2 Directory Structure
xkcd_project/
├── ingest_py           # Python ingestion script
├── airflow_dag.py             # Airflow DAG (copy to ~/airflow/dags/)
├── README.md                  # Project overview
├── execution_intructions.md         # This file
└── dbt/                       # dbt project
├── dbt_project.yml
├── models/
│   ├── staging/
│   │   └── saging.sql
│   ├── marts/
│   │   ├── dim_comic.sql
│   │   ├── dim_date.sql
│   │   └── fact_comic.sql
│   └── schema.yml
└── README.md
PART 2: GCP & BIGQUERY SETUP
2.1 Create a GCP Project

Go to https://console.cloud.google.com
Create a new project (e.g., "xkcd-pipeline")
Enable BigQuery API

2.2 Create a Service Account

Go to IAM & Admin → Service Accounts
Create new service account (e.g., "xkcd-pipeline-sa")
Grant roles:

BigQuery Admin (for creating datasets/tables)
BigQuery Data Editor (for inserting/querying data)


Create a JSON key and download it
Save to: ~/.gcp/xkcd-sa-key.json (or your preferred location)

2.3 Create BigQuery Datasets
Run these commands in BigQuery console or via gcloud:
sql-- Raw data layer
CREATE SCHEMA IF NOT EXISTS `your-project-id.xkcd_raw`
OPTIONS(description="Raw XKCD data from API");

-- Analytical layer
CREATE SCHEMA IF NOT EXISTS `your-project-id.xkcd_marts`
Change your-project-id to your actual GCP project ID.

PART 3: PYTHON INGESTION SETUP
3.1 Install Dependencies
bashpip install requests google-cloud-bigquery

3.2 Configure the Script
Edit ingest.py:
Line 26:
PROJECT_ID = "your-project-id"  ← Change to your GCP project

3.3 Set Environment Variable
bashexport GOOGLE_APPLICATION_CREDENTIALS=~/.gcp/xkcd-sa-key.json

3.4 Test the Ingestion Script
First time - create the table:
bashpython ingest.py --mode setup
Expected output: "Setup complete. Dataset and table ready."

Then do a full historical backfill (one-time):
bashpython ingest.py --mode historical
This fetches all 3,200+ XKCD comics and loads them.
Expected time: 5-10 minutes (due to time buffer between API calls)

(Optional) Verify in BigQuery:
sqlSELECT COUNT(*) as total_comics FROM `your-project-id.xkcd_raw.raw_comics`;
-- Should return: 3,200+

Test incremental mode:
bashpython ingest.py --mode incremental
This fetches only the latest comic. If the comic from today was pulled through the historical fill, delete using the below query, it may take some time due to stream buffering.

Once deleted, run the incremental again, it should complete in <2 seconds

PART 4: AIRFLOW SETUP

4.1 Install Airflow
bashpip install apache-airflow

4.2 Initialize Airflow
bashairflow db init

4.3 Create Airflow User
bashairflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

4.4 Copy DAG File
bashmkdir -p ~/airflow/dags
cp airflow_dag.py ~/airflow/dags/airflow_dag.py

4.5 Update DAG File
Edit ~/airflow/dags/airflow_dag.py
Line 13:
sys.path.insert(0, '/path/to/ingest/')  ← Update to your project path

4.6 Test the DAG
bashairflow dags list
Should see: xkcd_ingestion listed

# Test a single run:
airflow dags test xkcd_ingestion 2026-01-01
Expected output:

poll_for_new_comic task: PASSED
ingest_latest_comic task: PASSED
ingestion_success task: PASSED

PART 5: DBT SETUP

5.1 Install dbt
bashpip install dbt-bigquery

5.2 Configure BigQuery Connection
Create ~/.dbt/profiles.yml:
yamlxkcd:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: your-project-id
      dataset: xkcd_marts
      keyfile: ~/.gcp/xkcd-sa-key.json
      location: US
      threads: 4
Change:

your-project-id: Your actual GCP project
keyfile path: Your service account key location

5.3 Test Connection
bashcd dbt
dbt debug
# Should output: All checks passed!

5.4 Run dbt Models
bashdbt run
Expected output:

1 ephemeral (temp table) model compiled (staging)
3 table models created:

dim_comic (3,200+ rows)
dim_date (2,000+ rows)
fact_comic_metrics (3,200+ rows)



5.5 Verify Tables in BigQuery
sqlSELECT * FROM `your-project-id.xkcd_marts.dim_comic` LIMIT 5;
SELECT * FROM `your-project-id.xkcd_marts.dim_date` LIMIT 5;
SELECT * FROM `your-project-id.xkcd_marts.fact_comic_metrics` LIMIT 5;

