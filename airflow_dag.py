#Ingestion DAG

#Runs Monday, Wednesday, Friday, fetching and loading the latest XKCD comics into BigQuery.
#Includes a polling sensor to check for new comics

from datetime import datetime, timedelta
import logging
from select import poll
import sys
import os 

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor 

sys.path.insert(0, '/Users/aadeshmehra/Desktop/Skip') 

from ingest import ingest_historical, ingest_incremental, setup, get_bq_client, get_exisiting_comic_nums, fetch_comic

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'engineer',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 1, 1)
}

#defining dag
dag = DAG(
    'xkcd_ingestion',
    default_args=default_args,
    description='Ingest XKCD comics into BigQuery',
    schedule_interval='0 0 * * 1,3,5', # minute 0, at 12am, every Monday, Wednesday, Friday at midnight
    catchup=False,
    tags=['xkcd', 'ingestion'],
)

def polling_new_comic():
    latest = fetch_comic()
    if not latest:
        logger.warning("Failed to fetch latest comic during polling.")
        return False
    
    latest_num = latest["num"]

    client = get_bq_client()
    existing_nums = get_exisiting_comic_nums(client)

    if latest_num not in existing_nums:
        logger.info(f"Latest comic retrieved.")
        return True
    
    else: 
        logger.info(f"No new comic found during polling. Latest comic is {latest_num}.")    
        return False
    
#define task #1 to poll for new comic
poll_task = PythonSensor(
    task_id='poll_for_new_comic',
    python_callable=polling_new_comic,
    poke_interval=1800, #check every 30 minutes
    timeout=86400, #timeout after 24 hour
    mode = 'poke',
    dag=dag
)

#task 2: ingest latest comic - only happens after poll is true
ingest_task = PythonOperator(
    task_id='ingest_latest_comic',
    python_callable=ingest_incremental,
    dag=dag,
)

#task 3: Success message so we know ingestion has completed
def success_message(**context):
    ti = context['task_instance'] #got this from online docs for getting task info
    ti.log.info("Ingestion complete.")

success_task = PythonOperator(
    task_id='ingestion_success',
    python_callable=success_message,
    dag=dag,
)

poll_task >> ingest_task >> success_task
