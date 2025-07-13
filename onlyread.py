from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='gcs_read_only_example',  # Changed DAG ID to reflect read-only
    default_args=default_args,
    description='Un DAG para leer información desde Google Cloud Storage', # Updated description
    schedule_interval=None,
    catchup=False,
    tags=['gcs', 'read-only', 'example'], # Updated tags
) as dag:

    # --- Tarea 1: Leer información desde GCS ---
    def read_from_gcs_callable(**kwargs):
        ti = kwargs['ti']
        bucket_name = 'airflow-insumos'
        source_blob_name = 'data/input-data.txt'
        gcs_hook = GCSHook()
        
        # The download method can write to a file or return content
        file_content = gcs_hook.download(
            bucket_name=bucket_name,
            object_name=source_blob_name,
            filename='/tmp/downloaded_data.txt'  # Descarga a un archivo temporal
        )
    )

    # No further tasks are defined as this DAG is for read-only purposes.
    # The 'read_gcs_task' is the only task in this DAG.
