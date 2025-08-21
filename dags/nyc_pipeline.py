from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from nyc_taxi_datalake.tasks.nyc_task import fetch_taxi_data, transform_taxi_data, store_taxi_data, export_taxi_report

default_args = {
    "owner": "sajib",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nyc_pipeline",
    default_args=default_args,
    description="Mini Data Lake with DuckDB + Parquet on NYC Taxi Data",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["duckdb", "parquet", "nyc_taxi"],
) as dag:

    # Create local ts_nodash variable
    ts_nodash = "{{ ts_nodash }}"
    
    extract = PythonOperator(
        task_id="fetch_taxi_data",
        python_callable=fetch_taxi_data,
        op_kwargs={
            "ts_nodash": ts_nodash,
            "url": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
        }
    )

    transform = PythonOperator(
        task_id="transform_taxi_data",
        python_callable=transform_taxi_data,
        op_kwargs={
            "ts_nodash": ts_nodash
        }
    )

    load = PythonOperator(
        task_id="store_taxi_data",
        python_callable=store_taxi_data,
        op_kwargs={
            "ts_nodash": ts_nodash
        }
    )

    export = PythonOperator(
        task_id="export_taxi_report",
        python_callable=export_taxi_report,
        op_kwargs={
            "ts_nodash": ts_nodash
        }
    )

    extract >> transform >> load >> export