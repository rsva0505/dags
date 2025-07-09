from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import (
    GCSListObjectsOperator,
    GCSCreateBucketOperator, # Si necesitas crear un bucket desde Airflow
    GCSDeleteBucketOperator, # Si necesitas borrar un bucket
)
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator

# Define tu bucket de GCS
GCS_BUCKET_NAME = "airflow-info" # Cambia esto a tu bucket real

def write_to_gcs_fn(ti):
    hook = GCSHook()
    file_content = "Hola desde Apache Airflow en GKE!"
    object_name = "my_test_file.txt"
    hook.upload(
        bucket_name=GCS_BUCKET_NAME,
        object_name=object_name,
        data=file_content.encode('utf-8') # Los datos deben estar en bytes
    )
    print(f"Archivo {object_name} escrito en gs://{GCS_BUCKET_NAME}")
    ti.xcom_push(key="gcs_object_name", value=object_name)

def read_from_gcs_fn(ti):
    hook = GCSHook()
    object_name = ti.xcom_pull(key="gcs_object_name", task_ids="write_to_gcs")
    if object_name:
        file_content_bytes = hook.download(
            bucket_name=GCS_BUCKET_NAME,
            object_name=object_name
        )
        file_content = file_content_bytes.decode('utf-8')
        print(f"Contenido del archivo {object_name} leído: {file_content}")
    else:
        print("No se pudo obtener el nombre del objeto de GCS.")

with DAG(
    dag_id="gcs_read_write_example",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["gcs", "kubernetes"],
) as dag:
    # Opcional: Crear el bucket si no existe (solo para demostración, ten cuidado en producción)
    # create_gcs_bucket = GCSCreateBucketOperator(
    #     task_id="create_gcs_bucket",
    #     bucket_name=GCS_BUCKET_NAME,
    #     project_id="YOUR_PROJECT_ID", # Reemplaza con tu Project ID
    #     storage_class="STANDARD",
    #     location="US-CENTRAL1", # Reemplaza con tu ubicación
    #     labels={"airflow_test": "true"},
    # )

    write_to_gcs = PythonOperator(
        task_id="write_to_gcs",
        python_callable=write_to_gcs_fn,
    )

    list_gcs_objects = GCSListObjectsOperator(
        task_id="list_gcs_objects",
        bucket=GCS_BUCKET_NAME,
        prefix="my_test", # Filtra por un prefijo si es necesario
    )

    read_from_gcs = PythonOperator(
        task_id="read_from_gcs",
        python_callable=read_from_gcs_fn,
    )

    # Opcional: Borrar el bucket al final (solo para demostración)
    # delete_gcs_bucket = GCSDeleteBucketOperator(
    #     task_id="delete_gcs_bucket",
    #     bucket_name=GCS_BUCKET_NAME,
    #     gcp_conn_id="google_cloud_default", # Asegúrate de que esta conexión exista
    # )

    # Define el flujo de tareas
    # create_gcs_bucket >> write_to_gcs >> list_gcs_objects >> read_from_gcs # >> delete_gcs_bucket
    write_to_gcs >> list_gcs_objects >> read_from_gcs