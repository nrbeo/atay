import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

START_DATE = pendulum.datetime(2025, 1, 1, tz="UTC")

with DAG(
    dag_id="dag_transformation",
    description="Nettoyage, transformation et enrichissement des données",
    start_date=START_DATE,
    schedule=None,
    catchup=False,
    max_active_tasks=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["transformation", "pipeline2"],
) as dag:

    start = EmptyOperator(task_id="start_transformation")

    load_raw_data = EmptyOperator(task_id="load_raw_data")
    clean_age_data = EmptyOperator(task_id="clean_age_data")
    clean_life_data = EmptyOperator(task_id="clean_life_data")
    merge_datasets = EmptyOperator(task_id="merge_datasets")
    compute_additional_features = EmptyOperator(task_id="compute_additional_features")
    save_to_staging = EmptyOperator(task_id="save_to_staging")

    end = EmptyOperator(task_id="end_transformation")

    # Dépendances
    start >> [load_raw_data] 
    load_raw_data >> [clean_age_data, clean_life_data]
    [clean_age_data, clean_life_data] >> merge_datasets
    merge_datasets >> compute_additional_features >> save_to_staging >> end
