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
    dag_id="crypto_arb_pipeline",
    default_args=default_args,
    description="Orchestrates crypto arbitrage ingestion, streaming, and analytics",
    start_date=datetime(2026, 1, 1),
    schedule=None,   # manual trigger for now
    catchup=False,
    tags=["crypto", "streaming", "airflow"],
) as dag:

    start_binance = BashOperator(
        task_id="start_binance_ingestion",
        bash_command="python ingestion/binance_ws.py",
    )

    start_coinbase = BashOperator(
        task_id="start_coinbase_ingestion",
        bash_command="python ingestion/coinbase_ws.py",
    )

    start_spark = BashOperator(
        task_id="start_spark_normalization",
        bash_command="spark-submit streaming/normalize_l2.py",
    )

    run_analytics = BashOperator(
        task_id="run_downstream_analytics",
        bash_command='echo "Run Snowflake analytics queries here"',
    )

    [start_binance, start_coinbase] >> start_spark >> run_analytics