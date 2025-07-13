from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook # Usar hooks directamente
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
    dag_id='gcs_read_only_example',
    default_args=default_args,
    description='Un DAG para leer información desde Google Cloud Storage',
    schedule_interval=None,
    catchup=False,
    tags=['gcs', 'read-only', 'example'],
) as dag:

    # --- Tarea 1: Leer información desde GCS ---
    def read_from_gcs_callable(**kwargs):
        # ti = kwargs['ti'] # No es necesario si no vas a usar XComs o push/pull
        bucket_name = 'airflow-insumos'
        source_blob_name = 'data/input-data.txt'
        gcs_hook = GCSHook() # Instanciamos el hook

        logging.info(f"Intentando leer el blob '{source_blob_name}' del bucket '{bucket_name}'...")

        try:
            # Opción 1: Descargar el contenido directamente como string
            # Esto es lo más común si solo quieres el contenido para procesarlo en memoria
            file_content = gcs_hook.download(
                bucket_name=bucket_name,
                object_name=source_blob_name,
            ).decode('utf-8') # El método download devuelve bytes, lo decodificamos a string

            logging.info("Contenido del archivo:")
            logging.info(file_content)
            
            # Opción 2 (alternativa si necesitas guardarlo en un archivo):
            # gcs_hook.download(
            #     bucket_name=bucket_name,
            #     object_name=source_blob_name,
            #     filename='/tmp/downloaded_data.txt' # Descarga a un archivo temporal
            # )
            # with open('/tmp/downloaded_data.txt', 'r') as f:
            #     file_content = f.read()
            # logging.info("Contenido del archivo descargado a /tmp/downloaded_data.txt:")
            # logging.info(file_content)

        except Exception as e:
            logging.error(f"Error al leer desde GCS: {e}")
            raise # Re-lanza la excepción para que la tarea falle si hay un error

    read_gcs_task = PythonOperator(
        task_id='read_gcs_data',
        python_callable=read_from_gcs_callable,
        provide_context=True, # Necesario si usas **kwargs para ti, aunque aquí no se usa directamente
    )

    # No further tasks are defined as this DAG is for read-only purposes.
    # The 'read_gcs_task' is the only task in this DAG.