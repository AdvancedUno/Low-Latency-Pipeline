# src/dags/crypto_arb_pipeline.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "project_team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="crypto_arb_pipeline_test",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    start_binance = BashOperator(
        task_id="start_binance_ingestion",
        bash_command="python ingestion/binance_ws.py",
        retries=2,
        retry_delay=timedelta(minutes=2),
    )

    start_coinbase = BashOperator(
        task_id="start_coinbase_ingestion",
        bash_command="python ingestion/coinbase_ws.py",
        retries=2,
        retry_delay=timedelta(minutes=2),
    )

    start_spark = BashOperator(
        task_id="start_spark_normalization",
        bash_command="spark-submit streaming/normalize_l2.py",
        retries=2,
        retry_delay=timedelta(minutes=2),
    )

    run_analytics = BashOperator(
        task_id="run_downstream_analytics",
        bash_command='python analytics/run_queries.py',
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=10),
    )

    [start_binance, start_coinbase] >> start_spark >> run_analytics