from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from utils.snowflake_data_ingestion import load_to_snowflake
from utils.dbt_wrapper import create_dbt_task

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1)
}

def load_housing_data(**context):
    batch_id, num_rows = load_to_snowflake()
    context['task_instance'].xcom_push(key='batch_id', value=batch_id)
    context['task_instance'].xcom_push(key='num_rows', value=num_rows)


with DAG(
    'housing_data_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # Task 1: Data Ingestion
    ingest_data = PythonOperator(
        task_id='ingest_housing_data',
        python_callable=load_housing_data
    )

    # Task 2: Run dbt models
    dbt_snapshot = create_dbt_task('dbt_snapshot', 'snapshot', dag)
    dbt_run = create_dbt_task('dbt_run', 'run', dag)
    dbt_test = create_dbt_task('dbt_test', 'test', dag)
    dbt_docs = create_dbt_task('dbt_docs_generate', 'docs generate', dag)

    copy_docs = BashOperator(
        task_id='copy_dbt_docs',
        bash_command='''
        mkdir -p /usr/local/airflow/docs &&
        cp -rv /usr/local/airflow/include/housing_analytics/target/* /usr/local/airflow/docs/
        ''',
        dag=dag
    )


    ingest_data >> dbt_snapshot >> dbt_run >> dbt_test>> dbt_docs >> copy_docs
