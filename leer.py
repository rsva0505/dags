import os
from google.cloud import storage

def leer_archivo_de_gcs_especifico():
    """
    Lee un archivo específico ('data/input-data.txt') del bucket 'airflow-info'
    en Google Cloud Storage.
    """
    # --- Configuración Específica ---
    # Los nombres del bucket y del archivo están definidos directamente aquí
    # ya que nos los has proporcionado.
    GCS_BUCKET_NAME = "airflow-info"
    # La ruta completa del archivo dentro del bucket, incluyendo el directorio.
    GCS_FILE_PATH = "input-data.txt"

    try:
        # Inicializa un cliente de GCS
        client = storage.Client()

        # Obtiene el bucket
        bucket = client.get_bucket(GCS_BUCKET_NAME)

        # Obtiene el blob (el archivo) usando la ruta completa
        blob = bucket.blob(GCS_FILE_PATH)

        # Verifica si el archivo existe en el bucket
        if not blob.exists():
            print(f"Error: El archivo '{GCS_FILE_PATH}' no se encontró en el bucket '{GCS_BUCKET_NAME}'.")
            return None

        # Descarga el contenido del blob como texto
        contenido = blob.download_as_text()
        print(f"Contenido de '{GCS_FILE_PATH}' del bucket '{GCS_BUCKET_NAME}':")
        print("-" * 30)
        print(contenido)
        print("-" * 30)
        return contenido

    except Exception as e:
        print(f"Error al leer el archivo '{GCS_FILE_PATH}' del bucket '{GCS_BUCKET_NAME}': {e}")
        return None

if __name__ == "__main__":
    leer_archivo_de_gcs_especifico()
