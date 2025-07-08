from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from datetime import datetime

with DAG(
    dag_id='upload_sololeer_py_to_gcs',
    start_date=datetime(2023, 1, 1), # Puedes ajustar la fecha de inicio
    schedule_interval=None, # Este DAG se ejecutaría manualmente o con un trigger específico
    catchup=False,
    tags=['gcs', 'upload'],
) as dag:
    upload_file_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_sololeer_script',
        src='c:/users/rafael.vazquez/downloads/values/sololeer.py', # Ruta local del archivo
        dst='data/sololeer.py', # Ruta de destino en el bucket (directorio/nombre_archivo)
        bucket='airflow-info', # Nombre de tu bucket de GCS
        gcp_conn_id='google_cloud_default', # ID de la conexión de GCP configurada en Airflow
    )