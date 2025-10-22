import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

START_DATE = pendulum.datetime(2025, 1, 1, tz="UTC")

with DAG(
    dag_id="dag_ingestion",
    description="Ingestion des datasets bruts (Age Dataset + WHO Life Expectancy)",
    start_date=START_DATE,
    schedule=None,
    catchup=False,
    max_active_tasks=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ingestion", "pipeline1"],
) as dag:

    # Étapes logiques du pipeline
    start = EmptyOperator(task_id="start_ingestion")

    download_age_dataset = EmptyOperator(task_id="download_age_dataset")
    download_life_expectancy_dataset = EmptyOperator(task_id="download_life_expectancy_dataset")
    validate_files = EmptyOperator(task_id="validate_files")
    move_to_raw_zone = EmptyOperator(task_id="move_to_raw_zone")

    end = EmptyOperator(task_id="end_ingestion")

    # Dépendances
    start >> [download_age_dataset, download_life_expectancy_dataset]
    [download_age_dataset, download_life_expectancy_dataset] >> validate_files
    validate_files >> move_to_raw_zone >> end
