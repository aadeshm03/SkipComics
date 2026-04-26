# SkipComics
## XKCD Data Pipeline

This project builds an automated data pipeline that pulls comic data from the XKCD API, loads the raw data into BigQuery, and transforms it into analytics-ready tables using dbt.

### Pipeline Overview

The pipeline follows this flow:

API data is fetched from XKCD, loaded into a raw BigQuery table, transformed with dbt, and then validated using sql data quality checks.

```text
XKCD API → BigQuery Raw Table → dbt Transformations → Analytics Tables