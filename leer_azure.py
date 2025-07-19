from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="leer_escribir_insumos",
    start_date=pendulum.datetime(2025, 7, 18, tz="UTC"),
    catchup=False,
    schedule=None, # Para ejecutarlo manualmente
    tags=["insumos", "ejemplo"],
) as dag:
    
    # Define la ruta base donde se montó el volumen
    insumos_path = "/opt/airflow/insumos"

    def leer_y_escribir_archivo():
        """
        Lee el archivo de valores y lo escribe en un nuevo archivo.
        """
        import os

        # Define las rutas completas de los archivos
        archivo_de_lectura = os.path.join(insumos_path, "airflow_values.yaml")
        archivo_de_escritura = os.path.join(insumos_path, "archivo.txt")

        # Verifica que el archivo de origen exista
        if not os.path.exists(archivo_de_lectura):
            raise FileNotFoundError(f"El archivo {archivo_de_lectura} no fue encontrado. Asegúrate de que existe en tu volumen de insumos.")

        # Lee el contenido del archivo de origen
        with open(archivo_de_lectura, "r") as f:
            contenido = f.read()

        # Escribe el contenido en el nuevo archivo
        with open(archivo_de_escritura, "w") as f:
            f.write(contenido)
        
        print(f"Contenido de {archivo_de_lectura} escrito correctamente en {archivo_de_escritura}")

    # Define la tarea que ejecutará la función Python
    tarea_lectura_escritura = PythonOperator(
        task_id="leer_y_escribir_en_volumen",
        python_callable=leer_y_escribir_archivo,
    )