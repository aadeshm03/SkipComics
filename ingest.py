"""
Ingestion Script 

- historical 
- incremental

"""

import argparse
import logging
import requests
from typing import Optional

PROJECT_ID = 'skip-comics'
DATASET_ID    = "skip-comics.skipcomics"       
TABLE_ID      = "raw comics" 
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}" 
 
XKCD_BASE_URL    = "https://xkcd.com" 

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
    
def transform_comic(raw: dict) -> dict:

    year = int(raw.get("year", 0))
    month = int(raw.get("month", 0))
    day = int(raw.get("day", 0))

    

def main():
    parser = argparse.ArgumentParser(description="Fetch XKCD comic metadata.")
    parser.add_argument(
        "--comic",
        type=int,
        default=None,
        help="Comic number to fetch. Defaults to the latest comic.",
    )
    args = parser.parse_args()

    comic = fetch_comic(args.comic)
    if comic is None:
        raise SystemExit(1)

    logger.info("Fetched comic %s: %s", comic["num"], comic["title"])
    print(f'{comic["num"]}: {comic["title"]}')
    print(comic["img"])


if __name__ == "__main__":
    main()
