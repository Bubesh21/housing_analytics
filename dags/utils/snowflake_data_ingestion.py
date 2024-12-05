
import snowflake.connector
import pandas as pd
from datetime import datetime
import os
import yaml
from snowflake.connector.pandas_tools import write_pandas
import logging
import functools
import time
from typing import Callable,Any
from airflow.hooks.base import BaseHook


# Logging setup
def setup_logger(name: str) -> logging.Logger:
    """Configure logger with both file and console handlers"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Clear existing handlers if any
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Create handlers
    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(
        f"snowflake_loader_{datetime.now().strftime('%Y%m%d')}.log", 
        mode="a", 
        encoding="utf-8"
    )
    
    # Create formatter
    formatter = logging.Formatter(
        "{asctime} - {name} - {levelname} - {message}",
        style="{",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Set formatter for handlers
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

# Create logger
logger = setup_logger('snowflake_loader')

def log_execution(func: Callable) -> Callable:
    """Decorator to log function execution details"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.time()
        func_name = func.__name__
        logger.info(f"Starting execution of {func_name}")
        
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.info(f"Successfully completed {func_name} in {execution_time:.2f} seconds")
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Error in {func_name} after {execution_time:.2f} seconds: {str(e)}")
            raise
            
    return wrapper


@log_execution
def load_yaml():
    """Initialize with configuration from YAML"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, 'config.yaml')

    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

#@log_execution
# def connect_snowflake(config) -> snowflake.connector.SnowflakeConnection:
#     """Establish Snowflake connection"""
#     logger.info("Connecting to Snowflake Account")
#     try:
#         conn = snowflake.connector.connect(
#             user=config['user'],
#             password=config['password'],
#             account=config['account'],
#             warehouse=config['warehouse'],
#             database=config['database'],
#             schema=config['schema']
#         )
#         logger.info("Snowflake connection established successfully")
#         return conn
#     except Exception as e:
#         logger.error(f"Failed to connect to Snowflake: {str(e)}")
#         raise

@log_execution
def get_snowflake_connection():
    """Get Snowflake connection details from Airflow connection"""
    conn = BaseHook.get_connection('snowflake-conn')
    snowflake_config = {
        'user': conn.login,
        'password': conn.password,
        'account': conn.extra_dejson.get('account'),
        'warehouse': conn.extra_dejson.get('warehouse'),
        'database': conn.extra_dejson.get('database'),
        'schema': conn.extra_dejson.get('schema')
    }
    return snowflake_config


@log_execution
def combine_data(batch_id: int, load_ts: datetime, config) -> tuple:
    """Combine both housing datasets"""
    logger.info("Starting data combination process")
    
    # Read source files
    df_price = pd.read_csv(config['house_price'])
    df_full = pd.read_csv(config['housing_full'])
    
    # Convert column names to lowercase
    df_price.columns = df_price.columns.str.lower()
    df_full.columns = df_full.columns.str.lower()
    
    # Convert date format
    df_price['date'] = pd.to_datetime(df_price['date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
    df_full['date'] = pd.to_datetime(df_full['date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
        
    df_price['source_file'] = 'HOUSE_PRICE'
    df_full['source_file'] = 'HOUSING_FULL'
    df_price['batch_id'] = batch_id
    df_full['batch_id'] = batch_id
    df_price['insert_datetime'] = load_ts
    df_full['insert_datetime'] = load_ts
    
    # Combine datasets
    df_combined = pd.concat([df_price, df_full], ignore_index=True)
    
    # Save combined file 
    output_path = os.path.join(
        config['output_dir'],
        f'combined_housing_data_{batch_id}.csv'
    )
    df_combined.to_csv(output_path, index=False)
       
    return df_combined, output_path


@log_execution
def log_batch(cursor, conn, batch_id: int, step_name: str, status: str, 
             record_count: int = None, error_message: str = None) -> None:
    """Log batch information to Snowflake"""
    logger.info(f"Logging batch information - Batch ID: {batch_id}, Step: {step_name}, Status: {status}")
    try:
        cursor.execute("""
            INSERT INTO raw.batch_log (
                batch_id, process_name, step_name, status, 
                record_count, error_message, start_time
            ) VALUES (
                %s, 'DATA_LOAD', %s, %s, %s, %s, CURRENT_TIMESTAMP()
            )
        """, (batch_id, step_name, status, record_count, error_message))
        
        conn.commit()
        logger.info("Batch log updated successfully")
        
    except Exception as e:
        logger.error(f"Error updating batch log: {str(e)}")
        raise


@log_execution
def load_to_snowflake() -> tuple:
    """Main method to load data to Snowflake"""
    batch_id = int(datetime.now().strftime('%Y%m%d%H%M%S'))
    load_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    config = load_yaml()
    snowflake_config = get_snowflake_connection() 
    
    try:
        logger.info("Starting the pipeline")
        
        # Combine data
        df_combined, output_path = combine_data(batch_id,load_ts,config['file_paths'])
        
        conn =  conn = snowflake.connector.connect(
            user=snowflake_config['user'],
            password=snowflake_config['password'],
            account=snowflake_config['account'],
            warehouse=snowflake_config['warehouse'],
            database=snowflake_config['database'],
            schema=snowflake_config['schema']
        )
        logger.info("Created Snowflake connection")
        cursor = conn.cursor()
        logger.info("Snowflake connection established successfully")
        log_batch(cursor,conn,batch_id, 'DATA LOAD', 'RUNNING')
        
        # Load to Snowflake
        logger.info("Loading data to Snowflake")
        success, num_chunks, num_rows, _= write_pandas(
            conn=conn,
            df=df_combined,
            table_name=config['snowflake']['target_table_name'],
            schema=config['snowflake']['schema'],
            quote_identifiers=False,
            auto_create_table=False
        )
        
        if success:
            log_batch(
                cursor,
                conn,
                batch_id=batch_id,
                step_name='DATA LOAD',
                status='SUCCESS',
                record_count=num_rows
            )
            
            logger.info(f"""
            Load Complete:
            - Batch ID: {batch_id}
            - Records Loaded: {num_rows}
            - Output File: {output_path}
            """)
        
        return batch_id, num_rows
        
    except Exception as e:
        error_msg = f"Error in data load process: {str(e)}"
        logger.error(error_msg)
        if batch_id and cursor:
            log_batch(
                cursor,
                conn,
                batch_id=batch_id,
                step_name='DATA LOAD',
                status='ERROR',
                error_message=error_msg
            )
        raise
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logger.info("Snowflake connection closed")


load_to_snowflake()


