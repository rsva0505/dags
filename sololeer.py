from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

# Define el ID de tu proyecto de Google Cloud y el nombre del bucket
GCP_PROJECT_ID = "tu-proyecto-de-gcp"  # ¡IMPORTANTE: Reemplaza con el ID de tu proyecto!
GCS_BUCKET_NAME = "airflow-info"
GCS_FILE_NAME = "input-data.txt"

with DAG(
    dag_id="leer_archivo_gcs_k8s",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchups=False,
    tags=["gcs", "kubernetes", "data"],
    doc_md="""
    ### DAG para leer un archivo desde GCS usando KubernetesPodOperator

    Este DAG demuestra cómo leer un archivo específico (`input-data.txt`)
    desde un bucket de Google Cloud Storage (`airflow-info`) utilizando
    el `KubernetesPodOperator`. El operador crea un pod de Kubernetes
    que descarga el archivo y lo imprime en la salida estándar.
    """,
) as dag:
    read_gcs_file = KubernetesPodOperator(
        task_id="leer_archivo_desde_gcs",
        namespace="default",  # O el namespace donde quieres que se ejecute el pod
        image="google/cloud-sdk:latest",  # Imagen con gcloud y gsutil
        name="gcs-reader-pod",
        cmds=["bash", "-cx"],
        arguments=[
            f"gsutil cp gs://{GCS_BUCKET_NAME}/{GCS_FILE_NAME} /tmp/{GCS_FILE_NAME} && cat /tmp/{GCS_FILE_NAME}"
        ],
        do_xcom_push=False,
        is_delete_operator_pod=True,
        get_logs=True,
        # Si tu cluster de K8s está en GCP, el pod podría usar credenciales del
        # Workload Identity. De lo contrario, necesitarás configurar las credenciales
        # de servicio de GCP para el pod.
        # Por ejemplo, montando un secreto con una clave de cuenta de servicio:
        # volume_mounts=[
        #     V1VolumeMount(
        #         name="gcp-secret-volume",
        #         mount_path="/etc/gcp-secrets",
        #         read_only=True,
        #     ),
        # ],
        # volumes=[
        #     V1Volume(
        #         name="gcp-secret-volume",
        #         secret=V1SecretVolumeSource(secret_name="gcp-service-account-key"),
        #     ),
        # ],
        # env_vars={
        #    "GOOGLE_APPLICATION_CREDENTIALS": "/etc/gcp-secrets/key.json"
        # }
    )