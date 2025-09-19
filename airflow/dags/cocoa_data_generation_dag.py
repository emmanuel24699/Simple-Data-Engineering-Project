# Filename: dags/cocoa_data_generation_dag.py
import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Define the number of files to generate
NUM_FILES_TO_GENERATE = 3

# Define the absolute path to the scripts directory
SCRIPTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

# Add the scripts directory to the Python path
if SCRIPTS_DIR not in sys.path:
    sys.path.append(SCRIPTS_DIR)

# Import the function from your script
# noqa: E402
from generate_data import generate_and_save_multiple_files

def generate_and_upload_cocoa_data(**kwargs):
    """
    Generates multiple cocoa shipment files and uploads them to MinIO.
    """
    # Define bucket and prefix
    bucket_name = 'data'
    landing_prefix = 'landing'
    
    # Generate files and get the list of their paths using the variable
    local_file_paths = generate_and_save_multiple_files(
        num_files=NUM_FILES_TO_GENERATE
    )

    # Use S3Hook to upload each file to MinIO
    s3_hook = S3Hook(aws_conn_id='minio')
    
    for local_file_path in local_file_paths:
        file_name = os.path.basename(local_file_path)
        s3_key = f'{landing_prefix}/{file_name}'

        print(
            f"Uploading '{local_file_path}' to '{bucket_name}/{s3_key}'..."
        )
        s3_hook.load_file(
            filename=local_file_path,
            key=s3_key,
            bucket_name=bucket_name,
            replace=True
        )
        print("Upload complete.")
    
    # Clean up the local files after successful upload
    for local_file_path in local_file_paths:
        os.remove(local_file_path)


with DAG(
    dag_id='hourly_cocoa_data_generation_minio',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@hourly',
    catchup=False,
    tags=['data-generation', 'minio', 'cocoa'],
) as dag:
    
    generate_and_upload_task = PythonOperator(
        task_id='generate_and_upload_cocoa_data',
        python_callable=generate_and_upload_cocoa_data,
        provide_context=True,
    )