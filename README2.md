## Person 1 – Data Ingestion (Kaggle → S3 → RDS raw)

This part of the project is responsible for bringing the **raw Olist data** into our cloud environment.

### Components

- `notebooks/eda_olist.ipynb`  
  Exploratory analysis of the Olist Brazilian E-Commerce dataset.  
  Used to understand column types, missing values, and to design the raw schema.

- `ingestion/schema_raw.sql`  
  SQL DDL for the **raw layer in RDS**.  
  It creates one table per original CSV file (e.g., `customers_raw`, `orders_raw`, `order_items_raw`, etc.) with minimal constraints and foreign keys.

- `ingestion/download_from_kaggle.py`  
  Script that downloads the full Olist dataset from Kaggle  
  (`olistbr/brazilian-ecommerce`) into a local folder `DE_Project_Data/`.  
  This is the *Kaggle → local* step (no manual download from the UI).

- `ingestion/upload_to_s3.py`  
  Script that uploads all CSV files from `DE_Project_Data/` to the team S3 bucket:  
  `s3://de-27-team3/raw/`.  
  This is the *local → S3 (raw)* ingestion step.

- `ingestion/s3_to_rds.py`  
  Script that reads the raw CSV files from `s3://de-27-team3/raw/` and loads them  
  into the raw tables defined in `schema_raw.sql` in the team RDS instance.  
  The script uses `boto3` to read from S3 and `psycopg2` to insert into RDS.  
  RDS connection parameters are defined as placeholders and can be filled by the team.

- `ingestion/pipeline.py`  
  (Optional orchestrator) can be used as a single entry point to run the ingestion  
  steps in sequence (Kaggle → local → S3, and optionally S3 → RDS).

### How to reproduce the ingestion (for teammates)

1. **Kaggle → local**
```bash
python ingestion/download_from_kaggle.py
```
This will populate DE_Project_Data/ with the 9 CSV files.

2. **local → S3(raw)**
```bash
python ingestion/upload_to_s3.py
```
This uploads the CSV files to `s3://de-27-team3/raw/`

**<span style="color:red;">YOU MAY DELETE THE PART BELOW AFTER UPDATING THE FOLLOWING FILE</span>**\
PLEASE UPDATE THIS PART IN THE `ingestion/s3_to_rds.py` before going to step 3:
```bash
RDS_HOST = "<RDS_ENDPOINT>"        
RDS_PORT = 5432
RDS_DB   = "<DB_NAME>"             
RDS_USER = "<DB_USER>"
RDS_PWD  = "<DB_PASSWORD>"
```

3. **S3(raw) → RDS raw**
Update the RDS configuration placeholders in `ingestion/s3_to_rds.py`, then run:
```bash
python ingestion/s3_to_rds.py
```

