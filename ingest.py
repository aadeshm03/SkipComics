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

PROJECT_ID = 'skip-comics'
DATASET_ID    = "skip-comics.skipcomics"       
TABLE_ID      = "raw comics" 
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

def get_exisiting_comic_nums(client) -> set[int]

    query = f"SELECT DISTINCT num FROM `{FULL_TABLE_ID}`"

    try:
        result = client.query(query).result()
        return set(row.num for row in result)
    except NotFound:
        return set() #empty set if table doesnt exist
    

    
def main():
    parser = argparse.ArgumentParser(description="Fetch XKCD comic metadata.")
    parser.add_argument(
        "--comic",
        type=int,
        default=None,
        help="Comic number to fetch. Defaults to the latest comic.",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Print the raw XKCD API response instead of the transformed row.",
    )
    parser.add_argument(
        "--test-transformer",
        action="store_true",
        help="Run the transformer against a sample comic without calling the API.",
    )
    args = parser.parse_args()

    if args.test_transformer:
        sample_comic = {
            "num": 1,
            "title": "Barrel - Part 1",
            "safe_title": "Barrel - Part 1",
            "img": "https://imgs.xkcd.com/comics/barrel_cropped_(1).jpg",
            "transcript": "[[A boy sits in a barrel which is floating in an ocean.]]",
            "year": "2006",
            "month": "1",
            "day": "1",
            "news": "",
        }
        transformed = transform_comic(sample_comic)
        logger.info("Transformer test produced comic %s: %s", transformed["num"], transformed["title"])
        print(json.dumps(transformed, indent=2, sort_keys=True))
        return

    raw_comic = fetch_comic(args.comic)
    if raw_comic is None:
        raise SystemExit(1)

    logger.info("Fetched comic %s: %s", raw_comic["num"], raw_comic["title"])
    result = raw_comic if args.raw else transform_comic(raw_comic)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
