from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from fetch_news import fetch_news_data
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

default_args = {
    'owner': 'growdataskills',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'newsapi_to_gcs',
    default_args=default_args,
    description='Fetch news articles and save as Parquet in GCS',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 8, 3),
    catchup=False,
)

fetch_news_data_task = PythonOperator(
    task_id='newsapi_data_to_gcs',
    python_callable=fetch_news_data,
    dag=dag,
)

snowflake_create_table = SnowflakeOperator(
    task_id="snowflake_create_table",
    sql="""CREATE TABLE IF NOT EXISTS news_api_data USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(INFER_SCHEMA (
                    LOCATION => '@news_api.PUBLIC.gcs_raw_data_stage',
                    FILE_FORMAT => 'parquet_format'
                ))
            )""",
    snowflake_conn_id="snowflake_conn"
)

snowflake_copy = SnowflakeOperator(
    task_id="snowflake_copy_from_stage",
    sql="""COPY INTO news_api.PUBLIC.news_api_data 
            FROM @news_api.PUBLIC.gcs_raw_data_stage
            MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE 
            FILE_FORMAT = (FORMAT_NAME = 'parquet_format') 
            """,
    snowflake_conn_id="snowflake_conn"
)

fetch_news_data_task >> snowflake_create_table >> snowflake_copy