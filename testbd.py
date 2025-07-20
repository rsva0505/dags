from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

def test_db_connection():
    try:
        # Intenta obtener una conexion a la base de datos
        hook = PostgresHook(postgres_conn_id='at26')
        conn = hook.get_conn()
        conn.close()
        print("La conexión a la base de datos 'at26' fue exitosa.")
    except Exception as e:
        # Si ocurre un error, muestra el mensaje en los logs
        raise Exception(f"Error al conectar a la base de datos 'at26': {e}")

with DAG(
    dag_id='AT26_TEST_CONNECTION_DAG',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False
) as dag:
    
    test_connection_task = PythonOperator(
        task_id='test_db_connection_task',
        python_callable=test_db_connection
    )