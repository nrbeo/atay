from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from etl.transformation import (
    extract_ufo_landing,
    clean_ufo_staging,
    transform_ufo_staging,

    extract_gsod_landing,
    clean_gsod_staging,
    transform_gsod_staging,

    enrich_ufo_with_weather,
    build_star_schema_csv,
)

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_transformation",
    description="Transformation : staging UFO & GSOD, enrichissement, star schema",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # exécution manuelle
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["transformation", "staging", "star-schema", "pipeline2"],
):

    # -----------------------------
    # UFO PIPELINE (staging)
    # -----------------------------

    t_extract_ufo = PythonOperator(
        task_id="extract_ufo_landing",
        python_callable=extract_ufo_landing,
    )

    t_clean_ufo = PythonOperator(
        task_id="clean_ufo_staging",
        python_callable=clean_ufo_staging,
    )

    t_transform_ufo = PythonOperator(
        task_id="transform_ufo_staging",
        python_callable=transform_ufo_staging,
    )

    # -----------------------------
    # GSOD PIPELINE (staging)
    # -----------------------------

    t_extract_gsod = PythonOperator(
        task_id="extract_gsod_landing",
        python_callable=extract_gsod_landing,
    )

    t_clean_gsod = PythonOperator(
        task_id="clean_gsod_staging",
        python_callable=clean_gsod_staging,
        execution_timeout=timedelta(hours=4)
    )

    t_transform_gsod = PythonOperator(
        task_id="transform_gsod_staging",
        python_callable=transform_gsod_staging,
    )

    # -----------------------------
    # ENRICHMENT (join UFO + weather)
    # -----------------------------

    t_enrich = PythonOperator(
        task_id="enrich_ufo_with_weather",
        python_callable=enrich_ufo_with_weather,
        execution_timeout=timedelta(hours=1),
    )

    # -----------------------------
    # STAR SCHEMA BUILDING
    # -----------------------------

    t_build_star = PythonOperator(
        task_id="build_star_schema_csv",
        python_callable=build_star_schema_csv,
        execution_timeout=timedelta(hours=1),
    )

    # ---------------------------------
    # DAG DEPENDENCIES (pipeline graph)
    # ---------------------------------

    # UFO chain
    t_extract_ufo >> t_clean_ufo >> t_transform_ufo

    # GSOD chain
    t_extract_gsod >> t_clean_gsod >> t_transform_gsod

    # Merge pipelines at enrichment
    [t_transform_ufo, t_transform_gsod] >> t_enrich

    # Build star schema
    t_enrich >> t_build_star
