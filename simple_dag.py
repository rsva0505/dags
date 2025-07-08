from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='simple_test_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['test'],
) as dag:
    task1 = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello Airflow!"',
    )

    task2 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    task1 >> task2