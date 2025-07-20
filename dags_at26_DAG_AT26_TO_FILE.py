from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
import time
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime
import os
import tempfile
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.email import EmailOperator


def AT26_ATSUDEBAN_TOFILE(**kwargs):
	
	# Conexion a la bd at26
	hook = PostgresHook(postgres_conn_id='at26')
	
	sql_query_deftxt = '''
	CREATE TABLE IF NOT EXISTS FILE_AT.ATS_TH_AT26 (
		NROFRAUDE              VARCHAR(20),
		CAUSAFRAUDE            VARCHAR(20),
		TIPOINSTRUMENTO        VARCHAR(2),
		CANALFRAUDE            VARCHAR(255),
		TIPOFRAUDE             VARCHAR(2),
		TIPOFRANQUICIA         VARCHAR(255),
		MONTOFRAUDE            VARCHAR(15),
		AFECTACIONMONETARIA    VARCHAR(2),
		CODIGOCONTABLE         VARCHAR(20),
		TIPOPERSONA            VARCHAR(50),
		IDCLIENTE              VARCHAR(50),
		GENERO                 VARCHAR(50),
		RANGOEDAD              VARCHAR(50),
		PENSIONADOIVSS         VARCHAR(50),
		NRORECLAMO             VARCHAR(20),
		FECHAFRAUDE            VARCHAR(8),
		CODIGOPARROQUIA        VARCHAR(8),
		NOMBRECLIENTE          VARCHAR(255),
		TIPOOPERACION          VARCHAR(50),
		MONTOFRAUDEINTERNO     VARCHAR(15),
		MONTOFRAUDEEXTERNO     VARCHAR(15),
		RED                    VARCHAR(50)
	); '''
	
	hook.run(sql_query_deftxt)
	
	# vaciar la tabla antes de cargar
	sql_query_deftxt = '''TRUNCATE TABLE FILE_AT.ATS_TH_AT26;'''
	hook.run(sql_query_deftxt)

	
	# Insertar los registros en la tabla de destino
	sql_query_deftxt = '''
	INSERT INTO FILE_AT.ATS_TH_AT26 (
		NROFRAUDE,
		CAUSAFRAUDE,
		TIPOINSTRUMENTO,
		CANALFRAUDE,
		TIPOFRAUDE,
		TIPOFRANQUICIA,
		MONTOFRAUDE,
		AFECTACIONMONETARIA,
		CODIGOCONTABLE,
		TIPOPERSONA,
		IDCLIENTE,
		GENERO,
		RANGOEDAD,
		PENSIONADOIVSS,
		NRORECLAMO,
		FECHAFRAUDE,
		CODIGOPARROQUIA,
		NOMBRECLIENTE,
		TIPOOPERACION,
		MONTOFRAUDEINTERNO,
		MONTOFRAUDEEXTERNO,
		RED
	) 
	SELECT
		SUBSTRING(NROFRAUDE FROM 1 FOR 20) AS NROFRAUDE,
		CAUSAFRAUDE,
		TIPOINSTRUMENTO,
		CANALFRAUDE,
		TIPOFRAUDE,
		TIPOFRANQUICIA,
		CASE 
			WHEN MONTOFRAUDE = 0 THEN '0,00'
			ELSE REPLACE(TO_CHAR(MONTOFRAUDE, 'FM9999999999999.00'), '.', ',')
		END AS MONTOFRAUDE,
		AFECTACIONMONETARIA,
		CODIGOCONTABLE,
		TIPOPERSONA,
		SUBSTRING(IDCLIENTE FROM 1 FOR 19) AS IDCLIENTE,
		GENERO,
		RANGOEDAD,
		PENSIONADOIVSS,
		NRORECLAMO,
		FECHAFRAUDE,
		LPAD(CODIGOPARROQUIA, 6, '0') AS CODIGOPARROQUIA,
		SUBSTRING(NOMBRECLIENTE FROM 1 FOR 100) AS NOMBRECLIENTE,
		TIPOOPERACION,
		CASE 
			WHEN MONTOFRAUDEINTERNO = 0 THEN '0,00'
			ELSE REPLACE(TO_CHAR(MONTOFRAUDEINTERNO, 'FM9999999999999.00'), '.', ',')
		END AS MONTOFRAUDEINTERNO,
		CASE 
			WHEN MONTOFRAUDEEXTERNO = 0 THEN '0,00'
			ELSE REPLACE(TO_CHAR(MONTOFRAUDEEXTERNO, 'FM9999999999999.00'), '.', ',')
		END AS MONTOFRAUDEEXTERNO,
		RED
	FROM ATSUDEBAN.AT26_TH_BC
	ORDER BY NROFRAUDE, TIPOINSTRUMENTO, CANALFRAUDE asc; '''

	hook.run(sql_query_deftxt)

def ATS_TH_AT26_TOTXT(**kwargs):
    # The 'kwargs' variable is available here, inside the function.
    
    # Connection to the at26 database
    hook = PostgresHook(postgres_conn_id='at26')

    # Retrieve variables
    FileAT = Variable.get('FileAT_at26')
    FileCodSupervisado = Variable.get('FileCodSupervisado')
    FechaFile = Variable.get('FechaFile')

    # Get records from the database
    kwargs['ti'].log.info("Obteniendo registros de la base de datos...")
    registros = hook.get_records("SELECT * FROM FILE_AT.ATS_TH_AT26;")
    kwargs['ti'].log.info(f"Se obtuvieron {len(registros)} registros.")

    # Define the output path. This must be inside the function.
    output_directory = '/opt/airflow/insumos'
    output_filename = f"dags_at26_DAG_AT26_TO_FILE/{FileAT}{FileCodSupervisado}{FechaFile}.txt"
    output_file_path = os.path.join(output_directory, output_filename)

    try:
        # The 'output_file_path' variable is now defined and can be used here.
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
        
        kwargs['ti'].log.info(f"Escribiendo datos a archivo local: {output_file_path}")
        with open(output_file_path, 'w', encoding='utf-8') as f:
            for row in registros:
                linea = "~".join(str(valor) if valor is not None else "" for valor in row)
                f.write(linea + "\n")
        
        kwargs['ti'].log.info(f"Archivo generado y guardado en el PVC: {output_file_path}")

    except Exception as e:
        # The 'kwargs' variable is also available here, inside the function.
        kwargs['ti'].log.error(f"Error durante la generacion del archivo: {str(e)}")
        import traceback
        kwargs['ti'].log.error("Traceback completo:\n" + traceback.format_exc())
        raise

###### DEFINICION DEL DAG ###### 

default_args = {
	'owner': 'airflow',
	'start_date': days_ago(1)
}

dag = DAG(dag_id='AT26_TO_FILE', default_args=default_args, schedule_interval=None, catchup=False)


AT26_ATSUDEBAN_TOFILE_task = PythonOperator(
	task_id='AT26_ATSUDEBAN_TOFILE_task',
	python_callable=AT26_ATSUDEBAN_TOFILE,
	provide_context=True,
	dag=dag
)

ATS_TH_AT26_TOTXT_task = PythonOperator(
	task_id='ATS_TH_AT26_TOTXT_task',
	python_callable=ATS_TH_AT26_TOTXT,
	provide_context=True,
	dag=dag
)

enviar_correo_task = EmailOperator(
    task_id="enviar_correo",
	to="airflowprueba2025@gmail.com",
    subject="DAG {{ dag.dag_id }} completado",
    html_content="""
        <h3>¡Hola!</h3>
        <p>El DAG <b>{{ dag.dag_id }}</b> finalizó correctamente, generando el reporte: {{ var.value.FileAT_at26 }}{{ var.value.FileCodSupervisado }}{{ var.value.FechaFile }}.txt</p>
    """,
    conn_id="email_conn",
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
AT26_ATSUDEBAN_TOFILE_task >> ATS_TH_AT26_TOTXT_task >> enviar_correo_task
