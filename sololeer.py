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
    dag_id='gcs_read_only_example', # Cambiado el ID del DAG para reflejar que es de solo lectura
    default_args=default_args,
    description='Un DAG para leer información de Google Cloud Storage',
    schedule_interval=None,
    catchup=False,
    tags=['gcs', 'example', 'read-only'],
) as dag:

    # --- Tarea 1: Leer información desde GCS ---
    def read_from_gcs_callable(**kwargs):
        ti = kwargs['ti']
        bucket_name = 'airflow-info'  # El bucket es 'airflow-info'
        source_blob_name = 'data/input_data.txt'  # El archivo es 'input_data.txt' dentro del directorio 'data'
        gcs_hook = GCSHook()
        # [cite_start]Se descarga el contenido del archivo a un archivo temporal [cite: 4]
        file_content = gcs_hook.download(
            bucket_name=bucket_name,
            object_name=source_blob_name,
            filename='/tmp/downloaded_data.txt'
        )
        with open('/tmp/downloaded_data.txt', 'r') as f:
            data = f.read()
            # [cite_start]Se registra el contenido leído [cite: 5]
            logging.info(f"Contenido leído de GCS:\n{data}")
            ti.xcom_push(key='raw_data', value=data) # Se empuja el contenido leído a XCom para futuras tareas si fuera necesario

    read_gcs_task = PythonOperator(
        task_id='read_data_from_gcs',
        python_callable=read_from_gcs_callable,
        provide_context=True,
    )