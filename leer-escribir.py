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
    dag_id='gcs_read_write_example',
    default_args=default_args,
    description='Un DAG para leer y escribir en Google Cloud Storage',
    schedule_interval=None,
    catchup=False,
    tags=['gcs', 'example'],
) as dag:

    # --- Tarea 1: Leer información desde GCS ---
    def read_from_gcs_callable(**kwargs):
        ti = kwargs['ti']
        bucket_name = 'airflow-info'  # Updated to 'airflow-info'
        source_blob_name = 'data/input_data.txt'  # Updated to include 'data/' directory
        gcs_hook = GCSHook()
        # The download method can write to a file or return content
        file_content = gcs_hook.download(
            bucket_name=bucket_name,
            object_name=source_blob_name,
            filename='/tmp/downloaded_data.txt'  # Descarga a un archivo temporal
        )
        with open('/tmp/downloaded_data.txt', 'r') as f:
            data = f.read()
            logging.info(f"Contenido leído de GCS:\n{data}")
            ti.xcom_push(key='raw_data', value=data)

    read_gcs_task = PythonOperator(
        task_id='read_data_from_gcs',
        python_callable=read_from_gcs_callable,
        provide_context=True,
    )

    # --- Tarea 2: Procesar la información ---
    def process_data_callable(**kwargs):
        ti = kwargs['ti']
        raw_data = ti.xcom_pull(key='raw_data', task_ids='read_data_from_gcs')

        # Aquí es donde harías tu lógica de procesamiento.
        # Por ejemplo, convertir el texto a mayúsculas y añadir una marca de tiempo.
        processed_data = f"DATOS PROCESADOS ({dag.start_date}):\n{raw_data.upper()}\nFIN DEL PROCESAMIENTO."
        logging.info(f"Contenido procesado:\n{processed_data}")
        ti.xcom_push(key='processed_data', value=processed_data)

    process_data_task = PythonOperator(
        task_id='process_the_data',
        python_callable=process_data_callable,
        provide_context=True,
    )

    # --- Tarea 3: Escribir información de vuelta a GCS ---
    def write_to_gcs_callable(**kwargs):
        ti = kwargs['ti']
        processed_data = ti.xcom_pull(key='processed_data', task_ids='process_the_data')
        bucket_name = 'airflow-info'  # Updated to 'airflow-info'
        destination_blob_name = 'output_data.txt'  # Name of the output file in GCS root

        gcs_hook = GCSHook()

        # Guarda el contenido procesado en un archivo temporal para subirlo
        with open('/tmp/processed_output.txt', 'w') as f:
            f.write(processed_data)

        # The upload method can upload a local file
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=destination_blob_name,
            filename='/tmp/processed_output.txt',
            mime_type='text/plain'
        )
        logging.info(f"Contenido escrito en gs://{bucket_name}/{destination_blob_name}")

    write_gcs_task = PythonOperator(
        task_id='write_data_to_gcs',
        python_callable=write_to_gcs_callable,
        provide_context=True,
    )

    # Definir el orden de las tareas
    read_gcs_task >> process_data_task >> write_gcs_task