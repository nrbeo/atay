"""
LOAD PostgreSQL
--------------------------------
Charge le schéma en étoile dans PostgreSQL
à partir des CSV présents dans data/curated/.

Tables créées :

  dim_date
  dim_location
  dim_shape
  dim_weather_station
  dim_frshtt
  fact_ufo_observation
  ufo_comments_raw
"""

import os
from pathlib import Path

CURATED_DATA_PATH = Path(os.getenv("CURATED_DATA_PATH", "/opt/airflow/data/curated"))


# ============================================================
# 🔌 FONCTION UTILITAIRE : CONNEXION À POSTGRES
# ============================================================
def get_pg_conn():
    """
    Connexion PostgreSQL basée sur le .env (docker-compose)
    """
    import psycopg2

    host = os.getenv("POSTGRES_HOST", "postgres")
    db = os.getenv("POSTGRES_WAREHOUSE_DB", "atay_dw")
    user = os.getenv("POSTGRES_USER", "airflow")
    pwd = os.getenv("POSTGRES_PASSWORD", "airflow")
    port = int(os.getenv("POSTGRES_PORT", "5432"))

    return psycopg2.connect(
        host=host,
        database=db,
        user=user,
        password=pwd,
        port=port
    )


# ============================================================
# 🧱 TÂCHE 1 — CRÉATION DU SCHÉMA (DDL)
# ============================================================
def create_warehouse_database():
    """
    Tâche Airflow #0 :
    Crée la base de données du Data Warehouse si elle n'existe pas encore.
    """
    import psycopg2
    import os

    # On se connecte à la base *Airflow*, pas à la target (qui n'existe pas encore)
    host = os.getenv("POSTGRES_HOST", "postgres")
    user = os.getenv("POSTGRES_USER", "airflow")
    pwd = os.getenv("POSTGRES_PASSWORD", "airflow")
    base_airflow = os.getenv("POSTGRES_DB", "airflow")
    warehouse_db = os.getenv("POSTGRES_WAREHOUSE_DB", "atay_dw")

    print(f"[INIT] Vérification de la base {warehouse_db}…")

    conn = psycopg2.connect(
        dbname=base_airflow, user=user, password=pwd, host=host
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Vérifier si la base existe
    cur.execute(
        f"SELECT 1 FROM pg_database WHERE datname = '{warehouse_db}';"
    )
    exists = cur.fetchone()

    if exists:
        print(f"[INIT] La base {warehouse_db} existe déjà ✔")
    else:
        print(f"[INIT] Création de la base {warehouse_db}…")
        cur.execute(f"CREATE DATABASE {warehouse_db};")
        print(f"[INIT] Base {warehouse_db} créée avec succès ✔")

    cur.close()
    conn.close()

    return f"[INIT] Database {warehouse_db} ready"

def create_tables():
    """
    Crée toutes les tables du star schema dans PostgreSQL.
    Si elles existent, elles sont écrasées (DROP + CREATE).
    """
    ddl = """
    DROP TABLE IF EXISTS fact_ufo_observation CASCADE;
    DROP TABLE IF EXISTS ufo_comments_raw CASCADE;
    DROP TABLE IF EXISTS dim_date CASCADE;
    DROP TABLE IF EXISTS dim_location CASCADE;
    DROP TABLE IF EXISTS dim_shape CASCADE;
    DROP TABLE IF EXISTS dim_weather_station CASCADE;
    DROP TABLE IF EXISTS dim_frshtt CASCADE;

    CREATE TABLE dim_date (
        date_key INTEGER PRIMARY KEY,
        full_date DATE,
        year INTEGER,
        month INTEGER,
        day INTEGER,
        quarter INTEGER,
        day_of_week INTEGER,
        is_weekend BOOLEAN,
        season TEXT
    );

    CREATE TABLE dim_location (
        location_key INTEGER PRIMARY KEY,
        city TEXT,
        state TEXT,
        country TEXT,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION
    );

    CREATE TABLE dim_shape (
        shape_key INTEGER PRIMARY KEY,
        shape TEXT,
        shape_category TEXT
    );

    CREATE TABLE dim_weather_station (
        station_key INTEGER PRIMARY KEY,
        station_id TEXT,
        station_name TEXT,
        station_latitude DOUBLE PRECISION,
        station_longitude DOUBLE PRECISION,
        station_elevation DOUBLE PRECISION
    );

    CREATE TABLE dim_frshtt (
        frshtt_key INTEGER PRIMARY KEY,
        fog BOOLEAN,
        rain BOOLEAN,
        snow BOOLEAN,
        hail BOOLEAN,
        thunder BOOLEAN,
        tornado BOOLEAN,
        blue_sky BOOLEAN,
        label TEXT
    );

    CREATE TABLE fact_ufo_observation (
        fact_id INTEGER PRIMARY KEY,
        date_key INTEGER REFERENCES dim_date(date_key),
        location_key INTEGER REFERENCES dim_location(location_key),
        shape_key INTEGER REFERENCES dim_shape(shape_key),
        station_key INTEGER REFERENCES dim_weather_station(station_key),
        frshtt_key INTEGER REFERENCES dim_frshtt(frshtt_key),
        duration_seconds DOUBLE PRECISION,
        temp_mean DOUBLE PRECISION,
        visibility_mean DOUBLE PRECISION,
        comment_length DOUBLE PRECISION,
        has_comment BOOLEAN
    );

    CREATE TABLE ufo_comments_raw (
        fact_id INTEGER REFERENCES fact_ufo_observation(fact_id),
        comment_text TEXT
    );
    """

    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute(ddl)
    conn.commit()
    cur.close()
    conn.close()

    print("[LOAD] Tables PostgreSQL créées avec succès.")
    return "[LOAD] DDL OK"


# ============================================================
# 📥 Fonction générique de chargement via COPY
# ============================================================
def copy_csv_to_table(csv_path: Path, table_name: str):
    """
    Charge un CSV dans une table PostgreSQL via COPY FROM STDIN.
    """
    import psycopg2

    if not csv_path.exists():
        raise FileNotFoundError(f"[LOAD] Fichier introuvable : {csv_path}")

    conn = get_pg_conn()
    cur = conn.cursor()

    print(f"[LOAD] COPY → {table_name} depuis {csv_path.name}")

    with open(csv_path, "r", encoding="utf-8") as f:
        cur.copy_expert(
            f"COPY {table_name} FROM STDIN CSV HEADER", 
            f
        )

    conn.commit()
    cur.close()
    conn.close()

    return f"[LOAD] Table {table_name} chargée."


# ============================================================
#  DIMENSIONS
# ============================================================

def load_dim_date():
    return copy_csv_to_table(CURATED_DATA_PATH / "dim_date.csv", "dim_date")

def load_dim_location():
    return copy_csv_to_table(CURATED_DATA_PATH / "dim_location.csv", "dim_location")

def load_dim_shape():
    return copy_csv_to_table(CURATED_DATA_PATH / "dim_shape.csv", "dim_shape")

def load_dim_weather_station():
    return copy_csv_to_table(CURATED_DATA_PATH / "dim_weather_station.csv", "dim_weather_station")

def load_dim_frshtt():
    return copy_csv_to_table(CURATED_DATA_PATH / "dim_frshtt.csv", "dim_frshtt")


# ============================================================
#  FACT TABLE + COMMENTS
# ============================================================

def load_fact_ufo_observation():
    return copy_csv_to_table(
        CURATED_DATA_PATH / "fact_ufo_observation.csv",
        "fact_ufo_observation"
    )

def load_comments_raw():
    return copy_csv_to_table(
        CURATED_DATA_PATH / "ufo_comments_raw.csv",
        "ufo_comments_raw"
    )
