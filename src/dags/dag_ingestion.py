# src/dags/dag_ingestion.py

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from etl.ingestion import (
    download_ufo,
    download_gsod_archives,
    extract_gsod_archives,
    merge_gsod_years,
    insert_ufo_into_mongo,
    insert_gsod_into_mongo,
    clean_tmp_dirs 
)

default_args = {
    # "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_ingestion",
    description="Télécharge les données Kaggle (UFO + GSOD ZIP) et les insère dans MongoDB (landing_db)",
    start_date=datetime(2025, 1, 1),
    max_active_runs=1,
    schedule=None,  # exécution manuelle
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "kaggle", "mongo", "pipeline1"],
):

    t_download_ufo = PythonOperator(
        task_id="download_ufo",
        python_callable=download_ufo,
    )

    t_download_gsod_archives = PythonOperator(
        task_id="download_gsod_archives",
        python_callable=download_gsod_archives,
        execution_timeout=timedelta(hours=2),
    )

    t_extract_gsod_archives = PythonOperator(
        task_id="extract_gsod_archives",
        python_callable=extract_gsod_archives,
        execution_timeout=timedelta(hours=2),
    )

    t_merge_gsod_years = PythonOperator(
        task_id="merge_gsod_years",
        python_callable=merge_gsod_years,
        execution_timeout=timedelta(hours=3),
    )

    # t_insert_ufo = PythonOperator(
    #     task_id="insert_ufo_raw",
    #     python_callable=insert_ufo_into_mongo,
    # )

    # t_insert_gsod = PythonOperator(
    #     task_id="insert_gsod_raw",
    #     python_callable=insert_gsod_into_mongo
    # )

    t_clean_tmp = PythonOperator(
        task_id="clean_tmp_dirs",
        python_callable=clean_tmp_dirs,
    )


    # t_download_ufo >> t_insert_ufo

    # t_download_gsod_archives >> t_extract_gsod_archives >> t_merge_gsod_years >> t_insert_gsod


    t_download_gsod_archives >> t_extract_gsod_archives >> t_merge_gsod_years
    
    [t_download_ufo, t_merge_gsod_years] >> t_clean_tmp


# =============================================================================
# DAG 2: Offline Ingestion (skip downloads - uses pre-existing raw data)
# =============================================================================
# This DAG is designed for offline/local execution when raw data already exists
# in data/raw/. It skips download tasks but keeps extract, merge, and clean.
#
# Required files in data/raw/:
#   - ufo/complete.csv (or tmp_ufo/scrubbed.csv)
#   - tmp_gsod/archives/*.tar.gz (NOAA archives to extract)
#
# Use this DAG when:
#   - Running the pipeline offline for demo/evaluation
#   - Raw data has already been downloaded previously
#   - You want to skip the slow download steps

with DAG(
    dag_id="dag_ingestion_offline",
    description="Pipeline offline - extrait et merge les données NOAA déjà téléchargées (skip downloads)",
    start_date=datetime(2025, 1, 1),
    max_active_runs=1,
    schedule=None,  # exécution manuelle
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "offline", "local", "demo"],
):

    # Skip: t_download_ufo (UFO data already in data/raw/ufo/complete.csv)
    # Skip: t_download_gsod_archives (archives already in data/raw/tmp_gsod/archives/)

    t_extract_gsod_offline = PythonOperator(
        task_id="extract_gsod_archives",
        python_callable=extract_gsod_archives,
        execution_timeout=timedelta(hours=2),
    )

    t_merge_gsod_offline = PythonOperator(
        task_id="merge_gsod_years",
        python_callable=merge_gsod_years,
        execution_timeout=timedelta(hours=3),
    )

    t_clean_tmp_offline = PythonOperator(
        task_id="clean_tmp_dirs",
        python_callable=clean_tmp_dirs,
    )

    # Pipeline: extract NOAA archives → merge years → clean temp files
    t_extract_gsod_offline >> t_merge_gsod_offline >> t_clean_tmp_offline
