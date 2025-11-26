"""
s3_to_rds.py

Person1 - S3 -> RDS raw ingestion.

Reads the raw CSV files from S3 (de-27-team3/raw/*.csv)
and loads them into the raw tables defined in schema_raw.sql.
"""

import os
import tempfile
import boto3
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


# ========= S3 CONFIG =========
S3_BUCKET = "de-27-team3"
S3_PREFIX = "raw/"        
S3_REGION = "us-east-2"    
# =============================

# ========= RDS CONFIG (PLACEHOLDERS) =========
RDS_HOST = "<RDS_ENDPOINT>"        # e.g. "team3-db.xxxxxx.us-east-2.rds.amazonaws.com"
RDS_PORT = 5432
RDS_DB   = "<DB_NAME>"             # e.g. "olist_raw"
RDS_USER = "<DB_USER>"
RDS_PWD  = "<DB_PASSWORD>"
# ============================================

# Mapping from CSV file name to raw table name in schema_raw.sql
CSV_TABLE_MAP = {
    "olist_customers_dataset.csv": "customers_raw",
    "olist_geolocation_dataset.csv": "geolocation_raw",
    "olist_order_items_dataset.csv": "order_items_raw",
    "olist_order_payments_dataset.csv": "order_payments_raw",
    "olist_order_reviews_dataset.csv": "order_reviews_raw",
    "olist_orders_dataset.csv": "orders_raw",
    "olist_products_dataset.csv": "products_raw",
    "olist_sellers_dataset.csv": "sellers_raw",
    "product_category_name_translation.csv": "product_category_name_translation_raw",
}


def get_s3_client():
    """Create a boto3 S3 client."""
    return boto3.client("s3", region_name=S3_REGION)


def list_raw_csv_keys(s3_client):
    """
    List all CSV object keys under the raw/ prefix in the bucket.
    Returns a list of keys like 'raw/olist_customers_dataset.csv'.
    """
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv"):
                keys.append(key)
    return keys


def download_key_to_temp(s3_client, key, tmp_dir):
    """
    Download a single S3 object key into a temp directory.
    Returns the local file path.
    """
    filename = os.path.basename(key)
    local_path = os.path.join(tmp_dir, filename)
    s3_client.download_file(S3_BUCKET, key, local_path)
    return local_path


def get_rds_connection():
    """
    Create a psycopg2 connection to the RDS instance.

    NOTE: Person2/Person3 should fill in the correct host/db/user/password.
    """
    conn = psycopg2.connect(
        host=RDS_HOST,
        port=RDS_PORT,
        dbname=RDS_DB,
        user=RDS_USER,
        password=RDS_PWD,
    )
    conn.autocommit = True
    return conn


def load_csv_into_table(csv_path, table_name, conn):
    """
    Load a local CSV file into a given raw table.

    Assumes:
      - Column names in the CSV match the column names in schema_raw.sql.
      - Data is "raw" (no transform), we just insert as-is.
    """
    df = pd.read_csv(csv_path)

    if df.empty:
        print(f"[WARN] {csv_path} is empty, skipping.")
        return

    cols = list(df.columns)
    col_list_sql = ",".join(cols)

    # Build a list of tuples for execute_values
    records = [tuple(row[col] for col in cols) for _, row in df.iterrows()]

    with conn.cursor() as cur:
        # Optional: truncate table before load to avoid duplicates
        # cur.execute(f"TRUNCATE TABLE {table_name};")

        insert_sql = f"INSERT INTO {table_name} ({col_list_sql}) VALUES %s"
        execute_values(cur, insert_sql, records)

    print(f"[OK] Loaded {len(df)} rows from {os.path.basename(csv_path)} into {table_name}.")


def load_all_raw_tables():
    """
    High-level function:
    - list CSVs under raw/ in S3
    - download each to a temp dir
    - load into the corresponding raw table in RDS
    """
    s3 = get_s3_client()
    keys = list_raw_csv_keys(s3)

    if not keys:
        print("[WARN] No CSV objects found under raw/ in S3.")
        return

    # Temporary local directory to hold downloads
    with tempfile.TemporaryDirectory() as tmp_dir:
        print(f"[INFO] Using temp directory: {tmp_dir}")
        conn = get_rds_connection()

        try:
            for key in keys:
                filename = os.path.basename(key)
                if filename not in CSV_TABLE_MAP:
                    print(f"[WARN] {filename} not in CSV_TABLE_MAP, skipping.")
                    continue

                table_name = CSV_TABLE_MAP[filename]
                print(f"[INFO] Processing {filename} -> {table_name}")

                local_path = download_key_to_temp(s3, key, tmp_dir)
                load_csv_into_table(local_path, table_name, conn)
        finally:
            conn.close()
            print("[INFO] RDS connection closed.")


if __name__ == "__main__":
    # Entry point for running this script standalone
    print("Starting S3 -> RDS raw ingestion...")
    load_all_raw_tables()
    print("Done.")
