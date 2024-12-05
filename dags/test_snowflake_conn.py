from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': datetime(2024, 1, 1)
}

with DAG(
    'test_snowflake_connection',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    test_connection = SnowflakeOperator(
        task_id='test_connection',
        sql='SELECT current_version()',
        snowflake_conn_id='snowflake-conn' 
    )


def print_connection_details(**context):
    conn = BaseHook.get_connection('snowflake-conn')
    print(f"Extra: {conn.extra_dejson}")
    print(f"Schema: {conn.schema}")

check_conn = PythonOperator(
    task_id='check_connection',
    python_callable=print_connection_details,
    dag=dag
)