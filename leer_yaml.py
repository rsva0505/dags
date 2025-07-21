from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import yaml
import logging
import os

# Intenta importar GCSDownloadOperator.
# Si falla, se usará un placeholder o un mecanismo alternativo.
try:
    from airflow.providers.google.cloud.operators.gcs import GCSDownloadOperator
    GCS_OPERATOR_AVAILABLE = True
    logging.info("GCSDownloadOperator importado exitosamente.")
except ImportError as e:
    GCS_OPERATOR_AVAILABLE = False
    logging.warning(f"No se pudo importar GCSDownloadOperator: {e}. Se usará un método alternativo.")

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Nombre del bucket y archivo
GCS_BUCKET_NAME = 'airflow-insumos'
GCS_FILE_PATH = 'airflow_values-gcp.yaml'
LOCAL_FILE_PATH = '/tmp/airflow_values-gcp.yaml' # Ruta temporal dentro del pod de Airflow

def _download_file_alternative(bucket_name, object_name, local_file_path, **kwargs):
    """
    Función alternativa para descargar el archivo de GCS si GCSDownloadOperator no está disponible.
    Esto requeriría credenciales configuradas en el entorno (ej. service account montada, gcloud auth).
    """
    logging.warning("Usando método alternativo para descargar el archivo de GCS.")
    try:
        from google.cloud import storage
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        blob.download_to_filename(local_file_path)
        logging.info(f"Archivo {object_name} descargado a {local_file_path} usando google-cloud-storage.")
    except ImportError:
        logging.error("La librería google-cloud-storage no está instalada. No se puede descargar el archivo.")
        raise
    except Exception as e:
        logging.error(f"Error al descargar el archivo con el método alternativo: {e}")
        raise

def _process_yaml_file(ti):
    """
    Función Python para leer y procesar el archivo YAML descargado.
    """
    if not os.path.exists(LOCAL_FILE_PATH):
        logging.error(f"Error: El archivo {LOCAL_FILE_PATH} no se encontró. La descarga pudo haber fallado.")
        raise FileNotFoundError(f"Archivo esperado en {LOCAL_FILE_PATH} no encontrado.")

    try:
        with open(LOCAL_FILE_PATH, 'r') as file:
            data = yaml.safe_load(file)
            logging.info(f"Contenido del archivo YAML: {data}")
            ti.xcom_push(key='yaml_data', value=data)
    except yaml.YAMLError as exc:
        logging.error(f"Error al parsear el archivo YAML: {exc}")
        raise

with DAG(
    dag_id='gcs_read_yaml_k8s_with_fallback',
    default_args=default_args,
    description='Lee un archivo YAML de GCS con fallback si el operador nativo no está disponible.',
    schedule_interval=None,
    catchup=False,
    tags=['gcp', 'gcs', 'kubernetes', 'yaml', 'fallback'],
) as dag:

    if GCS_OPERATOR_AVAILABLE:
        download_yaml_file = GCSDownloadOperator(
            task_id='download_yaml_from_gcs',
            bucket_name=GCS_BUCKET_NAME,
            object_name=GCS_FILE_PATH,
            filename=LOCAL_FILE_PATH,
            gcp_conn_id='google_cloud_default',
        )
    else:
        # Si GCSDownloadOperator no está disponible, usamos el PythonOperator con la función alternativa
        # Esta función requerirá que la librería 'google-cloud-storage' esté instalada
        # y que las credenciales de GCP estén disponibles en el entorno (ej. Workload Identity)
        download_yaml_file = PythonOperator(
            task_id='download_yaml_from_gcs_alternative',
            python_callable=_download_file_alternative,
            op_kwargs={
                'bucket_name': GCS_BUCKET_NAME,
                'object_name': GCS_FILE_PATH,
                'local_file_path': LOCAL_FILE_PATH
            },
        )

    process_yaml_file = PythonOperator(
        task_id='process_yaml_file',
        python_callable=_process_yaml_file,
    )

    download_yaml_file >> process_yaml_file