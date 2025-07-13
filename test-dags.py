from __future__ import annotations

from datetime import datetime # Usamos datetime estándar
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSObjectExistenceSensor,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator

GCP_PROJECT_ID = "your-gcp-project-id"  # ¡REEMPLAZA ESTO!
GCS_BUCKET_NAME = "your-gcs-test-bucket-12345"  # ¡REEMPLAZA ESTO CON UN NOMBRE ÚNICO!
TEST_FILE_NAME = "test_airflow_gcs_file.txt"
LOCAL_TEST_FILE_PATH = f"/tmp/{TEST_FILE_NAME}"
TEST_FILE_CONTENT = "Hello from Airflow to GCP!"


with DAG(
    dag_id="gcs_read_write_verification_no_pendulum",
    start_date=datetime(2023, 1, 1, tzinfo=None), # Usamos datetime.datetime, sin tzinfo
    catchup=False,
    schedule=None,
    tags=["gcs", "verification", "test", "no-pendulum"],
    description="DAG para verificar lectura y escritura en GCS (sin pendulum)",
) as dag:
    # 

    # Tarea 1: Crear un archivo de prueba localmente
    create_local_file = BashOperator(
        task_id="create_local_test_file",
        bash_command=f"echo '{TEST_FILE_CONTENT}' > {LOCAL_TEST_FILE_PATH}",
    )

    # Tarea 2: Subir el archivo local a GCS
    upload_file_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_file_to_gcs",
        src=LOCAL_TEST_FILE_PATH,
        dst=TEST_FILE_NAME,
        bucket=GCS_BUCKET_NAME,
        project_id=GCP_PROJECT_ID,
    )

    # Tarea 3: Verificar que el archivo existe en GCS
    check_file_existence_in_gcs = GCSObjectExistenceSensor(
        task_id="check_file_existence_in_gcs",
        bucket=GCS_BUCKET_NAME,
        object=TEST_FILE_NAME,
        project_id=GCP_PROJECT_ID,
        mode="poke",
        poke_interval=5,
        timeout=60,
    )

    # Tarea 4: Descargar el archivo de GCS a una ubicación local temporal para lectura
    download_file_from_gcs = GCSToLocalFilesystemOperator(
        task_id="download_file_from_gcs",
        src_bucket=GCS_BUCKET_NAME,
        src_object=TEST_FILE_NAME,
        dst=f"{LOCAL_TEST_FILE_PATH}_downloaded",
        gcp_conn_id="google_cloud_default",
    )

    # Tarea 5: Leer el contenido del archivo descargado (para verificar la lectura)
    read_downloaded_file_content = BashOperator(
        task_id="read_downloaded_file_content",
        bash_command=f"cat {LOCAL_TEST_FILE_PATH}_downloaded && echo 'File content verified!'",
    )

    # Tarea 6: Eliminar el archivo de prueba de GCS
    delete_file_from_gcs = BashOperator(
        task_id="delete_file_from_gcs",
        bash_command=f"gsutil rm gs://{GCS_BUCKET_NAME}/{TEST_FILE_NAME}",
    )

    # Tarea 7: Eliminar los archivos locales de prueba
    clean_local_files = BashOperator(
        task_id="clean_local_files",
        bash_command=f"rm -f {LOCAL_TEST_FILE_PATH} {LOCAL_TEST_FILE_PATH}_downloaded",
    )

    # Definir el orden de las tareas
    (
        create_local_file
        >> upload_file_to_gcs
        >> check_file_existence_in_gcs
        >> download_file_from_gcs
        >> read_downloaded_file_content
        >> delete_file_from_gcs
        >> clean_local_files
    )