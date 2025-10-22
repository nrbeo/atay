import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

START_DATE = pendulum.datetime(2025, 1, 1, tz="UTC")

with DAG(
    dag_id="dag_production",
    description="Chargement dans la base PostgreSQL et génération des vues analytiques",
    start_date=START_DATE,
    schedule=None,
    catchup=False,
    max_active_tasks=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["production", "pipeline3"],
) as dag:

    start = EmptyOperator(task_id="start_production")

    create_star_schema = EmptyOperator(task_id="create_star_schema")
    load_staging_to_postgres = EmptyOperator(task_id="load_staging_to_postgres")
    create_analytics_views = EmptyOperator(task_id="create_analytics_views")
    run_statistics = EmptyOperator(task_id="run_statistics")
    export_curated_data = EmptyOperator(task_id="export_curated_data")

    end = EmptyOperator(task_id="end_production")

    # Dépendances
    start >> create_star_schema >> load_staging_to_postgres
    load_staging_to_postgres >> create_analytics_views >> run_statistics >> export_curated_data >> end
