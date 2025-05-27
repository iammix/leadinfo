# S3 to PostgreSQL ETL with Prefect

A Python ETL pipeline to **fetch CSVs from AWS S3**, validate/enrich data, and save to **PostgreSQL**, using **Prefect** for orchestration.

---

## Requirements

* Python 3.12+
* AWS S3 access & PostgreSQL instance
* Install deps:

  ```bash
  pip install boto3 pandas sqlalchemy psycopg2 prefect
  ```

---

## Setup

**1. Configure credentials in script:**

```python
AWS_ACCESS_KEY_ID = '...'
AWS_SECRET_ACCESS_KEY = '...'
AWS_REGION = '...'
BUCKET_NAME = '...'
POSTGRES_USER = '...'
POSTGRES_PASSWORD = '...'
POSTGRES_DB = '...'
POSTGRES_HOST = 'localhost'
POSTGRES_PORT = 5433
```

---

## Usage

**Run the pipeline:**

```bash
python main.py
```

* Processes all CSVs in the S3 bucket by default.
* To run on a single file: edit and call `main('file.csv')` in the script.

---

## What It Does

* Lists files in S3 bucket
* Downloads each CSV to pandas DataFrame
* Standardizes column names and validates fields (emails, Twitter, GitHub, LinkedIn)
* Enriches with `seniority`, `tech profile`, `multiple emails`
* Writes results to `enriched_data` table in PostgreSQL

---

## Customization

* Change/extend validators and enrichment logic in the respective functions.
* Change DB table in `save_df_to_postgresql()` if needed.
* Uncomment `@task` decorators for full Prefect orchestration.

---

## Notes

* Use environment variables or secrets management for real deployments.
* Ensure AWS & DB credentials are correct.
* Table is appended if it exists.

