from datetime import datetime, timedelta
import json
import logging
from io import BytesIO
from typing import List, Tuple

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

S3_CONN_ID = "minio"
PG_CONN_ID = "postgres_default"
MINIO_BUCKET = "data"
LANDING_PREFIX = "landing/"
STAGING_PREFIX = "staging/"
ARCHIVE_PREFIX = "archive/"
MANIFEST_PREFIX = "manifests/"
BATCH_SIZE = 5
CHUNK_SIZE = 50_000
PRICE_PER_KG_USD = 2.50

REQUIRED_COLUMNS = {
    "shipment_id",
    "timestamp",
    "farm_id",
    "region",
    "bean_type",
    "quality_score",
    "shipment_weight_kg",
    "temperature_celsius",
}

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="cocoa_supply_chain_dag",
    default_args=default_args,
    description="Scalable Cocoa Supply Chain Pipeline using manifest files",
    schedule_interval="@daily",
    start_date=datetime(2025, 9, 19),
    catchup=False,
    tags=["cocoa", "supply_chain"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    def check_for_files(**context):
        """
        Check if CSV files exist in the landing folder of S3.

        If files are found, a manifest JSON is created in S3
        listing all CSVs, which will be used by downstream tasks.

        Returns:
            str: Next task_id to execute ("generate_batches" or "no_files")
        """
        s3 = S3Hook(aws_conn_id=S3_CONN_ID)
        keys = s3.list_keys(bucket_name=MINIO_BUCKET, prefix=LANDING_PREFIX) or []
        csv_files = [k for k in keys if k.endswith(".csv")]

        if not csv_files:
            logging.info("No files found in landing zone. Exiting gracefully.")
            return "no_files"

        # Save manifest file in S3
        manifest_key = (
            f"{MANIFEST_PREFIX}manifest_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.json"
        )
        s3.load_string(
            string_data=json.dumps(csv_files),
            key=manifest_key,
            bucket_name=MINIO_BUCKET,
            replace=True,
        )

        context['ti'].xcom_push(key="manifest_key", value=manifest_key)
        return "generate_batches"

    check_files = BranchPythonOperator(
        task_id="check_for_files",
        python_callable=check_for_files,
    )

    no_files = EmptyOperator(task_id="no_files")

    @task(task_id="generate_batches")
    def generate_batches(**context) -> List[str]:
        """
        Read the manifest file from S3 and split the list of CSVs into batches.

        Each batch is saved as a separate JSON manifest in S3 for downstream processing.

        Returns:
            List[str]: List of S3 keys for batch manifests
        """
        s3 = S3Hook(aws_conn_id=S3_CONN_ID)
        manifest_key = context['ti'].xcom_pull(
            task_ids="check_for_files", key="manifest_key"
        )
        obj = s3.get_key(key=manifest_key, bucket_name=MINIO_BUCKET)
        csv_files = json.loads(obj.get()["Body"].read().decode())

        batches = [csv_files[i:i + BATCH_SIZE] for i in range(0, len(csv_files), BATCH_SIZE)]
        batch_manifest_keys = []

        for idx, batch in enumerate(batches):
            batch_key = (
                f"{MANIFEST_PREFIX}batch_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{idx}.json"
            )
            s3.load_string(
                string_data=json.dumps(batch),
                key=batch_key,
                bucket_name=MINIO_BUCKET,
                replace=True,
            )
            batch_manifest_keys.append(batch_key)

        return batch_manifest_keys

    @task(task_id="process_batch")
    def process_batch(batch_manifest_key: str) -> List[Tuple[str, str]]:
        """
        Process one batch of CSV files.

        Steps:
        1. Read batch manifest from S3.
        2. For each CSV:
           - Read in chunks.
           - Validate required columns.
           - Calculate shipment value in USD.
           - Save processed data to staging (Parquet in S3).
           - Load into Postgres with UPSERT (on shipment_id conflict).
        3. Return list of processed files for cleanup.

        Args:
            batch_manifest_key (str): S3 key of the batch manifest JSON

        Returns:
            List[Tuple[str, str]]: List of tuples of (landing_key, staging_key)
        """
        s3 = S3Hook(aws_conn_id=S3_CONN_ID)
        pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
        conn = pg.get_conn()
        cursor = conn.cursor()
        table = "cocoa_shipments"

        # Ensure table exists
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
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

        obj = s3.get_key(key=batch_manifest_key, bucket_name=MINIO_BUCKET)
        batch_files = json.loads(obj.get()["Body"].read().decode())

        processed_files = []

        for file_key in batch_files:
            try:
                obj = s3.get_key(key=file_key, bucket_name=MINIO_BUCKET)
                buffer = BytesIO(obj.get()["Body"].read())
                chunks = pd.read_csv(buffer, chunksize=CHUNK_SIZE)

                processed_parts = []

                for df in chunks:
                    missing_cols = REQUIRED_COLUMNS - set(df.columns)
                    if missing_cols:
                        logging.warning(f"Skipping {file_key}: missing columns {missing_cols}")
                        continue

                    # Calculate shipment value and mark processed timestamp
                    df["shipment_value_usd"] = round(df["shipment_weight_kg"] * PRICE_PER_KG_USD, 2)
                    df["processed_at"] = datetime.utcnow()
                    processed_parts.append(df)

                if not processed_parts:
                    continue

                final_df = pd.concat(processed_parts, ignore_index=True)
                staged_key = file_key.replace(LANDING_PREFIX, STAGING_PREFIX).replace(".csv", ".parquet")

                # Save to S3 as Parquet
                out_buffer = BytesIO()
                final_df.to_parquet(out_buffer, index=False)
                out_buffer.seek(0)
                s3.load_file_obj(out_buffer, bucket_name=MINIO_BUCKET, key=staged_key, replace=True)

                # Load into Postgres with UPSERT
                parquet_obj = BytesIO(s3.get_key(key=staged_key, bucket_name=MINIO_BUCKET).get()["Body"].read())
                df_pg = pd.read_parquet(parquet_obj)

                cursor.execute("DROP TABLE IF EXISTS tmp_cocoa_shipments")
                cursor.execute(f"CREATE TEMP TABLE tmp_cocoa_shipments AS TABLE {table} WITH NO DATA")
                conn.commit()

                buf = BytesIO()
                df_pg.to_csv(buf, index=False, header=False)
                buf.seek(0)
                cursor.copy_expert(f"COPY tmp_cocoa_shipments ({', '.join(df_pg.columns)}) FROM STDIN WITH CSV", buf)
                conn.commit()

                cursor.execute(f"""
                    INSERT INTO {table}
                    SELECT * FROM tmp_cocoa_shipments
                    ON CONFLICT (shipment_id) DO UPDATE SET
                        timestamp = EXCLUDED.timestamp,
                        farm_id = EXCLUDED.farm_id,
                        region = EXCLUDED.region,
                        bean_type = EXCLUDED.bean_type,
                        quality_score = EXCLUDED.quality_score,
                        shipment_weight_kg = EXCLUDED.shipment_weight_kg,
                        temperature_celsius = EXCLUDED.temperature_celsius,
                        shipment_value_usd = EXCLUDED.shipment_value_usd,
                        processed_at = EXCLUDED.processed_at
                """)
                conn.commit()

                processed_files.append((file_key, staged_key))

            except Exception as e:
                logging.error(f"Failed processing {file_key}: {e}")

        cursor.close()
        conn.close()
        return processed_files

    @task(task_id="cleanup_batch")
    def cleanup_batch(processed_files: List[Tuple[str, str]]):
        """
        Archive landing files and remove staging files from S3 after processing.

        Args:
            processed_files (List[Tuple[str, str]]): List of tuples of (landing_key, staging_key)
        """
        if not processed_files:
            return

        s3 = S3Hook(aws_conn_id=S3_CONN_ID)

        for landing_key, staging_key in processed_files:
            try:
                archive_key = landing_key.replace(LANDING_PREFIX, ARCHIVE_PREFIX)
                s3.copy_object(landing_key, archive_key, MINIO_BUCKET, MINIO_BUCKET)
                s3.delete_objects(MINIO_BUCKET, [landing_key])
                s3.delete_objects(MINIO_BUCKET, [staging_key])
            except Exception as e:
                logging.error(f"Cleanup failed for {landing_key}/{staging_key}: {e}")

    batches = generate_batches()
    processed_batches = process_batch.expand(batch_manifest_key=batches)
    cleanup_batches = cleanup_batch.expand(processed_files=processed_batches)

    start >> check_files
    check_files >> no_files
    check_files >> batches >> processed_batches >> cleanup_batches >> end
