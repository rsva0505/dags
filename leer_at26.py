from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='postgres_to_gcs_fraud_data',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['postgres', 'gcs', 'fraud_data'],
) as dag:
    check_postgres_connection = PostgresOperator(
        task_id='check_postgres_connection',
        postgres_conn_id='at26',  # ¡Actualizado a 't26'!
        sql="SELECT 1;",
    )

    extract_and_load_fraud_data = PostgresToGCSOperator(
        task_id='extract_and_load_fraud_data',
        postgres_conn_id='at26',  # ¡Actualizado a 't26'!
        sql="SELECT NROFRAUDE, CAUSAFRAUDE, TIPOINSTRUMENTO, CANALFRAUDE, TIPOFRAUDE, TIPOFRANQUICIA FROM your_table_name;", # Reemplaza 'your_table_name' con el nombre real de tu tabla
        bucket='airflow-insumos',
        filename='data/fraud_data_{{ ds }}.csv',
        export_format='csv',
        gcp_conn_id='google_cloud_default',
    )

    check_postgres_connection >> extract_and_load_fraud_data