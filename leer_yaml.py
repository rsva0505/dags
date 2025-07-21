from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSDownloadOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import yaml
import logging

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Nombre del bucket y archivo
GCS_BUCKET_NAME = 'airflow/insumos'  # Reemplaza si tu bucket tiene una estructura diferente
GCS_FILE_PATH = 'airflow_values-gcp.yaml'
LOCAL_FILE_PATH = '/tmp/airflow_values-gcp.yaml' # Ruta temporal dentro del pod de Airflow

def _process_yaml_file(ti):
    """
    Función Python para leer y procesar el archivo YAML descargado.
    """
    try:
        with open(LOCAL_FILE_PATH, 'r') as file:
            data = yaml.safe_load(file)
            logging.info(f"Contenido del archivo YAML: {data}")
            # Aquí puedes añadir tu lógica para procesar los datos del YAML.
            # Por ejemplo, podrías pasar estos datos a otro operador,
            # guardarlos en una base de datos, etc.
            ti.xcom_push(key='yaml_data', value=data)
    except FileNotFoundError:
        logging.error(f"Error: El archivo {LOCAL_FILE_PATH} no se encontró.")
        raise
    except yaml.YAMLError as exc:
        logging.error(f"Error al parsear el archivo YAML: {exc}")
        raise

with DAG(
    dag_id='gcs_read_yaml_k8s',
    default_args=default_args,
    description='Lee un archivo YAML de GCS en una implementación de Airflow en Kubernetes.',
    schedule_interval=None,
    catchup=False,
    tags=['gcp', 'gcs', 'kubernetes', 'yaml'],
) as dag:
    download_yaml_file = GCSDownloadOperator(
        task_id='download_yaml_from_gcs',
        bucket_name=GCS_BUCKET_NAME,
        object_name=GCS_FILE_PATH,
        filename=LOCAL_FILE_PATH,
        gcp_conn_id='google_cloud_default',  # Asegúrate de tener esta conexión configurada
    )

    process_yaml_file = PythonOperator(
        task_id='process_yaml_file',
        python_callable=_process_yaml_file,
    )

    download_yaml_file >> process_yaml_file