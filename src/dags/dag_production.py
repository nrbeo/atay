from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from etl.load_postgres import (
    create_warehouse_database,
    create_tables,
    load_dim_date,
    load_dim_location,
    load_dim_shape,
    load_dim_weather_station,
    load_dim_frshtt,
    load_fact_ufo_observation,
    load_comments_raw
)

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_load_postgres",
    description="Charge le schéma en étoile UFO dans PostgreSQL en multiples tâches",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["load", "postgres", "star-schema"],
):
    
    # 0 — Initialisation de la base de données du Data Warehouse
    t_init_db = PythonOperator(
        task_id="init_database",
        python_callable=create_warehouse_database,
    )

    # 1 — Création des tables
    t_create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
    )

    # 2 — Dimensions
    t_load_dim_date = PythonOperator(
        task_id="load_dim_date",
        python_callable=load_dim_date,
    )

    t_load_dim_location = PythonOperator(
        task_id="load_dim_location",
        python_callable=load_dim_location,
    )

    t_load_dim_shape = PythonOperator(
        task_id="load_dim_shape",
        python_callable=load_dim_shape,
    )

    t_load_dim_weather_station = PythonOperator(
        task_id="load_dim_weather_station",
        python_callable=load_dim_weather_station,
    )

    t_load_dim_frshtt = PythonOperator(
        task_id="load_dim_frshtt",
        python_callable=load_dim_frshtt,
    )

    # 3 — Fact table
    t_load_fact_ufo_observation = PythonOperator(
        task_id="load_fact_ufo_observation",
        python_callable=load_fact_ufo_observation,
    )

    # 4 — Commentaires
    t_load_comments_raw = PythonOperator(
        task_id="load_comments_raw",
        python_callable=load_comments_raw,
    )

    # Définition des dépendances entre les tâches
    # L'initialisation de la DB doit se faire en premier
    t_init_db >> t_create_tables
    # Les dimensions dépendent de la création des tables
    t_create_tables >> [
        t_load_dim_date,
        t_load_dim_location,
        t_load_dim_shape,
        t_load_dim_weather_station,
        t_load_dim_frshtt,
    ]

    # La fact table dépend des 5 dimensions
    [t_load_dim_date,
     t_load_dim_location,
     t_load_dim_shape,
     t_load_dim_weather_station,
     t_load_dim_frshtt] >> t_load_fact_ufo_observation

    # Les commentaires dépendent de la fact
    t_load_fact_ufo_observation >> t_load_comments_raw
