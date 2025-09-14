# Filename: airflow/dags/cocoa_processing_dag.py
import pandas as pd
import logging
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum
from typing import List
from io import StringIO

S3_CONN_ID = "minio"
PG_CONN_ID = "postgres_default"
MINIO_BUCKET = "data"

EXPECTED_COLUMNS = [
    "shipment_id",
    "timestamp",
    "farm_id",
    "region",
    "bean_type",
    "quality_score",
    "shipment_weight_kg",
    "temperature_celsius",
    "shipment_value_usd",
    "processed_at"
]

@dag(
    dag_id="cocoa_shipment_processing",
    start_date=pendulum.datetime(2025, 9, 13, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["cocoa", "etl"],
    doc_md="""### Cocoa Shipment Processing DAG"""
)
def cocoa_shipment_processing_dag():

    @task
    def list_new_files_from_minio() -> List[str]:
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        files = s3_hook.list_keys(bucket_name=MINIO_BUCKET, prefix="landing/")
        if not files:
            logging.error("No new files found in MinIO landing zone.")
            raise FileNotFoundError("No new files found in MinIO landing zone.")
        logging.info("Found %d new files: %s", len(files), files)
        return files

    @task
    def process_and_transform(file_key: str) -> str:
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        logging.info("Reading file from MinIO: %s", file_key)
        file_content = s3_hook.read_key(key=file_key, bucket_name=MINIO_BUCKET)
        df = pd.read_csv(StringIO(file_content))
        logging.info("Loaded %d rows from %s", len(df), file_key)

        # Handle missing values
        if "temperature_celsius" in df.columns:
            mean_temp = df["temperature_celsius"].mean()
            df["temperature_celsius"].fillna(mean_temp, inplace=True)
            logging.info("Filled missing 'temperature_celsius' values with mean: %.2f", mean_temp)

        # Transformations
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        price_per_kg_usd = 2.50
        df["shipment_value_usd"] = round(df["shipment_weight_kg"] * price_per_kg_usd, 2)
        df["processed_at"] = pendulum.now("UTC").isoformat()

        # Add missing columns
        for col in EXPECTED_COLUMNS:
            if col not in df.columns:
                df[col] = None
                logging.warning("Column '%s' missing in %s, filling with NULL.", col, file_key)

        # Keep only expected columns
        df = df[EXPECTED_COLUMNS]

        # Deduplicate
        before = len(df)
        df = df.drop_duplicates(subset=["shipment_id"])
        after = len(df)
        if before != after:
            logging.warning("Dropped %d duplicate shipment_ids in %s", before - after, file_key)

        logging.info("Processed %d unique records from %s", len(df), file_key)
        return df.to_json(orient="records")

    @task
    def load_data_to_postgres(data_json: str):
        pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        df = pd.read_json(StringIO(data_json))
        table_name = "cocoa_shipments"

        # Deduplicate within this batch
        df = df.drop_duplicates(subset=["shipment_id"])
        df = df[EXPECTED_COLUMNS]

        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        logging.info("Ensuring table '%s' exists in PostgreSQL.", table_name)
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                shipment_id TEXT PRIMARY KEY,
                timestamp TIMESTAMPTZ NOT NULL,
                farm_id TEXT,
                region TEXT,
                bean_type TEXT,
                quality_score NUMERIC,
                shipment_weight_kg NUMERIC,
                temperature_celsius NUMERIC,
                shipment_value_usd NUMERIC,
                processed_at TIMESTAMPTZ
            )
        """)
        conn.commit()

        # Prepare insert
        col_names = ", ".join(EXPECTED_COLUMNS)
        placeholders = ", ".join(["%s"] * len(EXPECTED_COLUMNS))
        insert_sql = f"""
            INSERT INTO {table_name} ({col_names})
            VALUES ({placeholders})
            ON CONFLICT (shipment_id) DO NOTHING
        """

        # Execute insert
        data_tuples = [tuple(x) for x in df.to_numpy()]
        cursor.executemany(insert_sql, data_tuples)
        conn.commit()

        inserted_count = cursor.rowcount  # actual new rows inserted
        attempted_count = len(df)         # deduplicated batch size

        if inserted_count < attempted_count:
            logging.warning(
                "Attempted to insert %d rows into '%s'. Only %d new rows were inserted (duplicates skipped).",
                attempted_count, table_name, inserted_count
            )
        else:
            logging.info(
                "Successfully inserted %d new rows into '%s'.",
                inserted_count, table_name
            )

        cursor.close()
        conn.close()

    @task
    def archive_file_in_minio(file_key: str):
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        destination_key = file_key.replace("landing/", "archive/")
        s3_hook.copy_object(
            source_bucket_key=file_key,
            dest_bucket_key=destination_key,
            source_bucket_name=MINIO_BUCKET,
            dest_bucket_name=MINIO_BUCKET,
        )
        s3_hook.delete_objects(bucket=MINIO_BUCKET, keys=file_key)
        logging.info("Archived '%s' to '%s'.", file_key, destination_key)

    # DAG flow
    files_to_process = list_new_files_from_minio()
    processed_data = process_and_transform.expand(file_key=files_to_process)
    load_to_db = load_data_to_postgres.expand(data_json=processed_data)
    archive_files = archive_file_in_minio.expand(file_key=files_to_process)

    load_to_db >> archive_files

cocoa_shipment_processing_dag()
