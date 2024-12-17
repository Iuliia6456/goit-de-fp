from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'data_pipeline_dag_iuliia',
    default_args=default_args,
    description='A data pipeline that processes data from landing to gold zone',
    schedule_interval=timedelta(days=1),  
    start_date=datetime(2024, 1, 1),  
    catchup=False,
    tags=['data_pipeline', 'spark', 'etl'],
) as dag:

    # Task 1: Landing to Bronze
    landing_to_bronze = SparkSubmitOperator(
        task_id='landing_to_bronze',
        application='dags/landing_to_bronze.py',  # Path relative to Airflow's home
        conn_id='spark-default',
        verbose=1,
        conf={
            'spark.master': 'local[*]',
        },
    )

    # Task 2: Bronze to Silver
    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        application='dags/bronze_to_silver.py',
        conn_id='spark-default',
        verbose=1,
        conf={
            'spark.master': 'local[*]',
        },
    )

    # Task 3: Silver to Gold
    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold',
        application='dags/silver_to_gold.py',
        conn_id='spark-default',
        verbose=1,
        conf={
            'spark.master': 'local[*]',
        },
    )

    # Define task dependencies
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
