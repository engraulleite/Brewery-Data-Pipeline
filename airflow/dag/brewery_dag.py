from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "bees_data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="brewery_bees",
    default_args=default_args,
    description="Open Brewery DB ETL Pipeline with Medallion Architecture",
    schedule_interval=None,
    start_date=datetime(2025, 1, 9),
    catchup=False,
    tags=["bees", "delta"],
) as dag:

    exec_bronze = BashOperator(
        task_id="bronze_etl",
        bash_command=(
            "docker exec -e data_process={{ ds }} spark-container "
            "python3 /home/project/scripts/exec_bronze.py"
        ),
    )

    exec_silver = BashOperator(
        task_id="silver_etl",
        bash_command=(
            "docker exec -e data_process={{ ds }} -e LOAD_MODE=delta -e DELTA_DAYS=2 "
            "-e PYTHONPATH=/home/project spark-container "
            "python3 /home/project/scripts/exec_silver.py"
        ),
    )

    exec_gold = BashOperator(
        task_id="gold_etl",
        bash_command=(
            "docker exec -e data_process={{ ds }} -e LOAD_MODE=delta -e DELTA_DAYS=2 "
            "-e PYTHONPATH=/home/project spark-container "
            "python3 /home/project/scripts/exec_gold.py"
        ),
    )

    exec_verify = BashOperator(
        task_id="exec_verific",
        bash_command=(
            "docker exec -e PYTHONPATH=/home/project -e data_process={{ ds }} "
            "spark-container python3 /home/project/tests/verify_all.py"
        ),
    )

    # Definindo as dependências entre as etapas do pipeline
    exec_bronze >> exec_silver >> exec_gold >> exec_verify
