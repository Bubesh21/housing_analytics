from airflow.operators.bash import BashOperator
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)

def get_dbt_env():
    """Returns common environment variables for dbt tasks"""
    logger.info("Retrieving dbt environment variables")
    try:
        env_vars = {
            'SNOWFLAKE_ACCOUNT': '{{ conn["snowflake-conn"].extra_dejson["account"] }}',
            'SNOWFLAKE_USER': '{{ conn["snowflake-conn"].login }}',
            'SNOWFLAKE_PASSWORD': '{{ conn["snowflake-conn"].password }}',
            'SNOWFLAKE_WAREHOUSE': '{{ conn["snowflake-conn"].extra_dejson["warehouse"] }}',
            'SNOWFLAKE_DATABASE': '{{ conn["snowflake-conn"].extra_dejson["database"] }}',
            'SNOWFLAKE_ROLE': '{{ conn["snowflake-conn"].extra_dejson["role"] }}',
            'SNOWFLAKE_SCHEMA': '{{ conn["snowflake-conn"].schema }}'
        }
        logger.info("Successfully retrieved dbt environment variables")
        return env_vars
    except Exception as e:
        logger.error(f"Error retrieving dbt environment variables: {str(e)}")
        raise

def create_dbt_task(task_id: str, dbt_command: str, dag) -> BashOperator:
    """Creates a dbt task with common configuration"""
    logger.info(f"Creating dbt task: {task_id} with command: {dbt_command}")
    
    base_cmd = '--profiles-dir /usr/local/airflow/include/housing_analytics --project-dir /usr/local/airflow/include/housing_analytics'
    full_command = f'dbt {dbt_command} {base_cmd}'
    
    logger.info(f"Full dbt command: {full_command}")
    
    return BashOperator(
        task_id=task_id,
        bash_command=f"""
            set -e  # Exit on error
            echo "Starting dbt task: {task_id}"
            echo "Command: {dbt_command}"
            echo "Timestamp: $(date)"
            {full_command}
            echo "Task completed successfully"
            echo "Timestamp: $(date)"
        """,
        env=get_dbt_env(),
        dag=dag
    )