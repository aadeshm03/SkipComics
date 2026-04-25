"""
Ingestion Script 

- historical 
- incremental

"""

import argparse
import json
import logging
import requests
from typing import Optional
from datetime import datetime, timezone
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from google.auth.exceptions import DefaultCredentialsError
import time

PROJECT_ID = 'skip-comics'
DATASET_ID    = "skipcomics"
TABLE_ID      = "raw_comics_cleaned"
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}" 
 
XKCD_BASE_URL    = "https://xkcd.com" 


#define raw schema for bigquery
RAW_COMICS_SCHEMA = [
    bigquery.SchemaField("num",          "INTEGER",   mode="REQUIRED", description="Unique comic number"),
    bigquery.SchemaField("title",        "STRING",    mode="NULLABLE", description="Comic title"),
    bigquery.SchemaField("safe_title",   "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("img",          "STRING",    mode="NULLABLE", description="URL to comic image"),
    bigquery.SchemaField("transcript",   "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("year",         "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("month",        "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("day",          "INTEGER",   mode="NULLABLE", description="Publish day"),
    bigquery.SchemaField("publish_date", "DATE",      mode="NULLABLE"),
    bigquery.SchemaField("news",         "STRING",    mode="NULLABLE", description="Extra news field from API"),
    bigquery.SchemaField("fetched_at",   "TIMESTAMP", mode="REQUIRED", description="When row was ingested"),
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

# Fetch single comic as a dictionary

def fetch_comic(comic_num: Optional[int] = None) -> Optional[dict]:

    if comic_num is None:
        url = f"{XKCD_BASE_URL}/info.0.json"
    else:
        url = f"{XKCD_BASE_URL}/{comic_num}/info.0.json"
    
    try:
        response = requests.get(url, timeout=20)

        if response.status_code == 404:
            logger.warning(f"Comic number {comic_num} not found.")
            return None
        
        response.raise_for_status()

        return response.json()
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching comic number {comic_num}: {e}")
        return None
    
def transform_comic(raw) -> dict:

    year = int(raw.get("year", 0))
    month = int(raw.get("month", 0))
    day = int(raw.get("day", 0))

    try:
        publish_date = datetime(year, month, day).date().isoformat()
    except ValueError:
        publish_date = None

    return {
        "num":          int(raw["num"]),            
        "title":        raw.get("title"),       
        "safe_title":   raw.get("safe_title"),
        "img":          raw.get("img"),
        "transcript":   raw.get("transcript") or None, 
        "year":         year or None,             
        "month":        month or None,
        "day":          day or None,
        "publish_date": publish_date,
        "news":         raw.get("news") or None,
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
    }
 
 #Big Query table set up 

def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)

#Create big query table if it doesn't exist
def table_exists(client) -> None:

    dataset = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")

    try:
        client.get_dataset(dataset)
    except NotFound:
        logger.info(f"Dataset {DATASET_ID} not found. Creating dataset.")
        client.create_dataset(dataset)

    #same for table 
    table = bigquery.Table(FULL_TABLE_ID, schema=RAW_COMICS_SCHEMA)
    
    try:
        client.get_table(FULL_TABLE_ID)
    except NotFound:
        logger.info(f"Table {TABLE_ID} not found. Creating table.")
        client.create_table(table)

def get_exisiting_comic_nums(client) -> set[int]:

    query = f"SELECT DISTINCT num FROM `{FULL_TABLE_ID}`"

    try:
        result = client.query(query).result()
        return set(row.num for row in result)
    except NotFound:
        return set() #empty set if table doesnt exist

#insert rows
def insert_rows(client, rows: list[dict]) -> int:
    if not rows:
        return 0
    
    errors = client.insert_rows_json(FULL_TABLE_ID, rows)

    if errors:
        logger.error(f"Error inserting rows: {errors}")
        return 0
    return len(rows)

#Historical fill of all comics
def historical_comics(client, max_comics: Optional[int] = None) -> None:
        
    logger.info("Begin historical ingestion.")

    latest = fetch_comic()
    if latest is None:
        logger.error("Could not fetch latest comic. Stopping historical ingestion.")
        raise SystemExit(1)

    latest_num = latest["num"]
    if max_comics is not None:
        latest_num = min(latest_num, max_comics)

    logger.info(f"Latest comic number is {latest_num}.")

    existing_nums = get_exisiting_comic_nums(client)
    logger.info(f"Found {len(existing_nums)} existing comics in BigQuery.") 

    batch: list[dict] = []
    inserted_count = 0

    for num in range(1, latest_num + 1):
    
        if num in existing_nums:
            continue

        raw = fetch_comic(num)
        time.sleep(0.2) #delay for requests

        if raw is None:
            continue

        batch.append(transform_comic(raw))

        if len(batch) >= 500:
            inserted = insert_rows(client, batch)
            inserted_count = inserted_count + inserted
            logger.info(f"Inserted batch")
            batch = []

    if batch:
        inserted = insert_rows(client, batch)
        inserted_count = inserted_count + inserted

    logger.info(f"Historical ingestion complete. Inserted {inserted_count} new comics.")

#incremental fetch for latest comic
def streaming_comic(client) -> None:
    raw = fetch_comic()
    if not raw:
        raise RuntimeError("Failed to fetch latest comic during streaming.")
    
    latest_num = raw["num"] 

    #check if comic already exists in bigquery
    existing_nums = get_exisiting_comic_nums(client)
    if latest_num in existing_nums:
        logger.info(f"Latest comic {latest_num} already exists in BigQuery. No new comic to insert.")
        return  
    
    row = transform_comic(raw)
    insert_rows(client, [row])   
    logger.info(f"Inserted latest comic {latest_num} into BigQuery.")

def setup():

    client = get_bq_client()
    table_exists(client)
    logger.info("Dataset and table ready.")

def ingest_historical():
    client = get_bq_client()
    historical_comics(client)

def ingest_incremental():
    client = get_bq_client()
    streaming_comic(client)
    
def main():

    parser = argparse.ArgumentParser(description="XKCD → BigQuery ingestion")
    parser.add_argument(
        "--mode",
        choices=["setup", "historical", "incremental"],
        default="incremental",
        help="setup = create tables (one-time); historical: load all comics; incremental = latest comic only",
    )
    args = parser.parse_args()
 
    if args.mode == "setup":
        setup()
    elif args.mode == "historical":
        ingest_historical()
    else:
        ingest_incremental()
 
if __name__ == "__main__":
    main()