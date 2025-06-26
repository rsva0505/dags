from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from airflow.sensors.filesystem import FileSensor # <-- ELIMINAR ESTA IMPORTACIÃ“N
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor # <-- AGREGAR ESTA IMPORTACIÃ“N
import time
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime


def AT_DIA_HABIL(**kwargs):
    hook = PostgresHook(postgres_conn_id='at26')
    sql_query = '''SELECT 0;'''
    result = hook.get_records(sql_query)
    Variable.set('AT_DIA_HABIL', result[0][0])

class HolidayCheckSensor(BaseSensorOperator):
    """
    Sensor que espera hasta que el dÃ­a actual NO sea feriado.
    La condicion se determina ejecutando una consulta SQL en la base de datos.
    """
    @apply_defaults
    def __init__(self, postgres_conn_id, *args, **kwargs):
        super(HolidayCheckSensor, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id

    def poke(self, context):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        sql_query = """
            SELECT CASE 
                        WHEN to_char(CURRENT_DATE, 'dd/mm/yy') IN (
                            SELECT to_char(df_fecha, 'dd/mm/yy') 
                            FROM ods.cl_dias_feriados 
                            WHERE SUBSTRING(df_year FROM 3 FOR 2) = SUBSTRING(to_char(CURRENT_DATE, 'dd/mm/yy') FROM 7 FOR 2)
                        )
                        THEN 1 
                        ELSE 0 
                    END AS status;
        """
        records = hook.get_records(sql_query)
        # Suponiendo que la consulta retorna 1 fila, 1 columna:
        status = records[0][0] if records else 0
        self.log.info("Valor de status (1=feriado, 0=no feriado): %s", status)
        # Esperamos que status sea 0 para continuar con el flujo normal
        return status == 0

def FileAT_at26(**kwargs):
    value = 'AT26'
    Variable.set('FileAT_at26', value)

def FileCodSupervisado(**kwargs):
    value = '01410'
    Variable.set('FileCodSupervisado', value)

def FechaInicio_M(**kwargs):
    hook = PostgresHook(postgres_conn_id='at26')
    sql_query = '''SELECT TO_CHAR(DATE_TRUNC('month', CURRENT_DATE - INTERVAL '28 days'), 'MM/DD/YYYY')'''
    result = hook.get_records(sql_query)
    Variable.set('FechaInicio_M', result[0][0])

def FechaFin_M(**kwargs):
    hook = PostgresHook(postgres_conn_id='at26')
    sql_query = '''SELECT TO_CHAR(DATE_TRUNC('month', CURRENT_DATE - INTERVAL '28 days') + INTERVAL '1 month - 1 day', 'mm/dd/yyyy');'''
    result = hook.get_records(sql_query)
    Variable.set('FechaFin_M', result[0][0])

def FechaFin(**kwargs):
    hook = PostgresHook(postgres_conn_id='at26')

    FechaFin_M = Variable.get('FechaFin_M')

    sql_query = f'''SELECT '{FechaFin_M}'; '''
    result = hook.get_records(sql_query)
    Variable.set('FechaFin', result[0][0])

def FechaInicio(**kwargs):
    hook = PostgresHook(postgres_conn_id='at26')

    FechaInicio_M = Variable.get('FechaInicio_M')

    sql_query = f'''SELECT '{FechaInicio_M}'; '''
    result = hook.get_records(sql_query)
    Variable.set('FechaInicio', result[0][0])

def FechaFile(**kwargs):
    hook = PostgresHook(postgres_conn_id='at26')
    
    FechaFin = Variable.get('FechaFin')

    sql_query = f'''SELECT TO_CHAR(TO_DATE('{FechaFin}', 'MM/DD/YY'), 'YYMMDD') AS result;'''
    result = hook.get_records(sql_query)
    Variable.set('FechaFile', result[0][0])

def FileDate(**kwargs):
    hook = PostgresHook(postgres_conn_id='at26')
    sql_query = '''SELECT TO_CHAR(CURRENT_DATE, 'YYMMDD');'''
    result = hook.get_records(sql_query)
    Variable.set('FileDate', result[0][0])    

###### DEFINICION DEL DAG ###### 

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

dag = DAG(dag_id='AT26_PRINCIPAL', default_args=default_args, schedule_interval=None, catchup=False)

# Tarea corregida para esperar el archivo en GCS
wait_for_file = GCSObjectExistenceSensor(
    task_id='wait_for_file',
    bucket='us-central1-composer-bancar-794f4b44-bucket', # <-- CAMBIO DE 'bucket_name' A 'bucket'
    object='data/AT26/INSUMOS/AT26_FRAUDE.csv',                       # <-- CAMBIO DE 'object_name' A 'object'
    poke_interval=10,
    timeout=60 * 10,
    dag=dag
)

AT_DIA_HABIL_task = PythonOperator(
    task_id='AT_DIA_HABIL_task',
    python_callable=AT_DIA_HABIL,
    provide_context=True,
    dag=dag
)

holiday_sensor = HolidayCheckSensor(
    task_id='holiday_sensor',
    postgres_conn_id='at26',  
    poke_interval=10,    # verificamos cada 10 segundos para la prueba pero se verifica al dia, 1 dia = 86400 segundos.
    dag=dag
)

FileAT_task = PythonOperator(
    task_id='FileAT_task',
    python_callable=FileAT_at26,
    provide_context=True,
    dag=dag
)

FileCodSupervisado_task = PythonOperator(
    task_id='FileCodSupervisado_task',
    python_callable=FileCodSupervisado,
    provide_context=True,
    dag=dag
)

FechaInicio_M_task = PythonOperator(
    task_id='FechaInicio_M_task',
    python_callable=FechaInicio_M,
    provide_context=True,
    dag=dag
)

FechaFin_M_task = PythonOperator(
    task_id='FechaFin_M_task',
    python_callable=FechaFin_M,
    provide_context=True,
    dag=dag
)

FechaFin_task = PythonOperator(
    task_id='FechaFin_task',
    python_callable=FechaFin,
    provide_context=True,
    dag=dag
)

FechaInicio_task = PythonOperator(
    task_id='FechaInicio_task',
    python_callable=FechaInicio,
    provide_context=True,
    dag=dag
)

FechaFile_task = PythonOperator(
    task_id='FechaFile_task',
    python_callable=FechaFile,
    provide_context=True,
    dag=dag
)

FileDate_task = PythonOperator(
    task_id='FileDate_task',
    python_callable=FileDate,
    provide_context=True,
    dag=dag
)

Execution_of_the_Scenario_AT26_task = TriggerDagRunOperator(
    task_id='Execution_of_the_Scenario_AT26_task',
    trigger_dag_id='AT26',  
    wait_for_completion=True,
    dag=dag
)

Execution_of_the_Scenario_AT26_TO_FILE_task = TriggerDagRunOperator(
    task_id='Execution_of_the_Scenario_AT26_TO_FILE_task',
    trigger_dag_id='AT26_TO_FILE',  
    wait_for_completion=True,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
wait_for_file >> AT_DIA_HABIL_task >> holiday_sensor >> FileAT_task >> FileCodSupervisado_task >> FechaInicio_M_task >> FechaFin_M_task >> FechaFin_task >> FechaInicio_task >> FechaFile_task >> FileDate_task >> Execution_of_the_Scenario_AT26_task >> Execution_of_the_Scenario_AT26_TO_FILE_task