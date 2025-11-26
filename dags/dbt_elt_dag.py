from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "you",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DBT_PROJECT_DIR = "/opt/airflow/dbt/stock_analytics"
DBT_PROFILES_DIR = "/opt/airflow/dbt/stock_analytics"
DBT = "/home/airflow/.local/bin/dbt"  # use the dbt on PATH inside the container

with DAG(
    dag_id="lab2_dbt_elt_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,      # triggered manually or by another DAG
    catchup=False,
    tags=["dbt", "elt"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT} run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT} test",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT} snapshot",
    )

    dbt_run >> dbt_test >> dbt_snapshot
