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

# =========================================================================
# MONGODB EXTRACTION IMPORTS (ALTERNATIVE)
# =========================================================================
# Uncomment these imports to use MongoDB as the landing zone instead of CSV.
# See etl/transformation.py for the function implementations.
#
# from etl.transformation import (
#     extract_ufo_from_mongo,
#     extract_gsod_from_mongo,
# )
# =========================================================================

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

    # -------------------------------------------------------------------------
    # ALTERNATIVE: Extract UFO from MongoDB instead of CSV
    # -------------------------------------------------------------------------
    # This task extracts UFO data from MongoDB landing zone (landing_db.ufo_raw)
    # instead of reading from the raw CSV file. Useful when MongoDB is used
    # as the primary landing store for real-time ingestion scenarios.
    #
    # To enable: comment out t_extract_ufo above, uncomment t_extract_ufo_mongo,
    # and update the DAG dependencies to use t_extract_ufo_mongo.
    # -------------------------------------------------------------------------
    # t_extract_ufo_mongo = PythonOperator(
    #     task_id="extract_ufo_from_mongo",
    #     python_callable=extract_ufo_from_mongo,
    # )
    # -------------------------------------------------------------------------

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

    # -------------------------------------------------------------------------
    # ALTERNATIVE: Extract GSOD from MongoDB instead of CSV
    # -------------------------------------------------------------------------
    # This task extracts GSOD weather data from MongoDB landing zone
    # (landing_db.gsod_raw) instead of reading from the merged CSV file.
    #
    # WARNING: This is VERY SLOW for large datasets. The GSOD dataset contains
    # 50+ million records spanning 1980-1990. Extracting from MongoDB
    # document-by-document is significantly slower than reading a CSV file.
    # This option is preserved to demonstrate MongoDB integration capability.
    #
    # To enable: comment out t_extract_gsod above, uncomment t_extract_gsod_mongo,
    # and update the DAG dependencies to use t_extract_gsod_mongo.
    # -------------------------------------------------------------------------
    # t_extract_gsod_mongo = PythonOperator(
    #     task_id="extract_gsod_from_mongo",
    #     python_callable=extract_gsod_from_mongo,
    #     execution_timeout=timedelta(hours=6),  # Extended timeout for large dataset
    # )
    # -------------------------------------------------------------------------

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
