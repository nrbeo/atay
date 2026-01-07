
"""
TRANSFORMATION PIPELINE (Pipeline 2)
===================================

Objectif :
- Partir des CSV de la landing zone (UFO + GSOD)
- Construire des fichiers de staging nettoyés
- Enrichir UFO avec la météo (station la plus proche + jour correspondant)
- Produire les fichiers CSV du star schema dans data/curated
"""

import os
from pathlib import Path

# -------------------------------------------------------------------
#  PATHS
# -------------------------------------------------------------------

BASE_DATA_PATH = Path(os.getenv("DATA_PATH", "/opt/airflow/data"))
RAW_DATA_PATH = BASE_DATA_PATH / "raw"
STAGING_DATA_PATH = BASE_DATA_PATH / "staging"
CURATED_DATA_PATH = BASE_DATA_PATH / "curated"

STAGING_DATA_PATH.mkdir(parents=True, exist_ok=True)
CURATED_DATA_PATH.mkdir(parents=True, exist_ok=True)


# ===================================================================
# ============  MONGODB EXTRACTION FUNCTIONS (ALTERNATIVE)  =========
# ===================================================================
#
# These functions extract data from MongoDB instead of CSV files.
# They are commented out because loading large datasets (50M+ rows for GSOD)
# into MongoDB is extremely time-consuming and not practical for batch analytics.
#
# The code is preserved to demonstrate that the architecture supports
# MongoDB as an alternative landing zone for scenarios requiring:
#   - Document-based access patterns
#   - Real-time ingestion with immediate queryability
#   - Schema flexibility for heterogeneous data
#
# To use MongoDB as landing zone:
#   1. Uncomment insert_ufo_into_mongo / insert_gsod_into_mongo in dag_ingestion.py
#   2. Uncomment extract_ufo_from_mongo / extract_gsod_from_mongo below
#   3. Uncomment corresponding tasks in dag_transformation.py
#   4. Replace extract_ufo_landing / extract_gsod_landing tasks with MongoDB versions
# ===================================================================

# def get_mongo_client():
#     """
#     Returns a MongoDB client connected to the landing database.
#     Credentials are read from environment variables.
#     """
#     from pymongo import MongoClient
#
#     user = os.getenv("MONGO_INITDB_ROOT_USERNAME", "mongoadmin")
#     pwd = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "mongopwd")
#     host = os.getenv("MONGO_HOST", "mongo")
#     port = int(os.getenv("MONGO_PORT", "27017"))
#     uri = f"mongodb://{user}:{pwd}@{host}:{port}/"
#     return MongoClient(uri)


# def extract_ufo_from_mongo():
#     """
#     Alternative UFO extraction from MongoDB landing zone.
#     ----------------------------------------------------
#     Reads UFO documents from MongoDB collection 'landing_db.ufo_raw'
#     and exports them to staging as CSV:
#         MongoDB (landing_db.ufo_raw) -> staging/ufo_extracted.csv
#
#     This function mirrors extract_ufo_landing() but sources data from
#     MongoDB instead of the raw CSV file.
#     """
#     import pandas as pd
#
#     out_path = STAGING_DATA_PATH / "ufo_extracted.csv"
#
#     print("[UFO-MONGO] Connecting to MongoDB...")
#     client = get_mongo_client()
#     coll = client["landing_db"]["ufo_raw"]
#
#     # Count documents for logging
#     doc_count = coll.count_documents({})
#     print(f"[UFO-MONGO] Found {doc_count} documents in landing_db.ufo_raw")
#
#     if doc_count == 0:
#         raise RuntimeError("[UFO-MONGO] No documents in MongoDB collection. Run ingestion first.")
#
#     # Fetch all documents (excluding MongoDB _id field)
#     print("[UFO-MONGO] Fetching documents...")
#     cursor = coll.find({}, {"_id": 0})
#     records = list(cursor)
#
#     # Convert to DataFrame and save as CSV
#     df = pd.DataFrame(records)
#     df.to_csv(out_path, index=False)
#
#     print(f"[UFO-MONGO] Extract OK: {len(df)} rows -> {out_path}")
#     client.close()
#
#     return f"[UFO-MONGO] Extracted {len(df)} rows from MongoDB"


# def extract_gsod_from_mongo():
#     """
#     Alternative GSOD extraction from MongoDB landing zone.
#     ------------------------------------------------------
#     Reads GSOD documents from MongoDB collection 'landing_db.gsod_raw'
#     and exports them to staging as CSV:
#         MongoDB (landing_db.gsod_raw) -> staging/gsod_extracted.csv
#
#     This function mirrors extract_gsod_landing() but sources data from
#     MongoDB instead of the raw CSV file.
#
#     WARNING: This is very slow for large datasets (50M+ documents).
#     The function uses batched cursor iteration to avoid memory issues.
#     """
#     import pandas as pd
#
#     out_path = STAGING_DATA_PATH / "gsod_extracted.csv"
#
#     print("[GSOD-MONGO] Connecting to MongoDB...")
#     client = get_mongo_client()
#     coll = client["landing_db"]["gsod_raw"]
#
#     # Count documents for logging
#     doc_count = coll.count_documents({})
#     print(f"[GSOD-MONGO] Found {doc_count} documents in landing_db.gsod_raw")
#
#     if doc_count == 0:
#         raise RuntimeError("[GSOD-MONGO] No documents in MongoDB collection. Run ingestion first.")
#
#     # Stream documents in batches to avoid memory overflow
#     BATCH_SIZE = 100_000
#     first_write = True
#     total_written = 0
#
#     print(f"[GSOD-MONGO] Streaming extraction in batches of {BATCH_SIZE}...")
#
#     # Use cursor with no_cursor_timeout for long operations
#     cursor = coll.find({}, {"_id": 0}).batch_size(BATCH_SIZE)
#
#     batch_records = []
#     for doc in cursor:
#         batch_records.append(doc)
#
#         if len(batch_records) >= BATCH_SIZE:
#             df_batch = pd.DataFrame(batch_records)
#             df_batch.to_csv(
#                 out_path,
#                 mode="w" if first_write else "a",
#                 header=first_write,
#                 index=False
#             )
#             total_written += len(batch_records)
#             first_write = False
#             batch_records = []
#             print(f"[GSOD-MONGO] Written {total_written} rows...")
#
#     # Write remaining records
#     if batch_records:
#         df_batch = pd.DataFrame(batch_records)
#         df_batch.to_csv(
#             out_path,
#             mode="w" if first_write else "a",
#             header=first_write,
#             index=False
#         )
#         total_written += len(batch_records)
#
#     print(f"[GSOD-MONGO] Extract OK: {total_written} rows -> {out_path}")
#     client.close()
#
#     return f"[GSOD-MONGO] Extracted {total_written} rows from MongoDB"


# ===================================================================
# ==========================  UFO PIPELINE  =========================
# ===================================================================

def extract_ufo_landing():
    """
    Étape UFO #1
    ------------
    Lit le fichier RAW UFO et le copie tel quel en staging :
        raw/ufo/complete.csv -> staging/ufo_extracted.csv
    """
    import pandas as pd

    raw_path = RAW_DATA_PATH / "ufo" / "complete.csv"
    out_path = STAGING_DATA_PATH / "ufo_extracted.csv"

    if not raw_path.exists():
        raise FileNotFoundError(f"[UFO] Fichier RAW introuvable : {raw_path}")

    print(f"[UFO] Extraction depuis RAW : {raw_path}")
    df = pd.read_csv(raw_path, on_bad_lines='skip', engine='python')
    df.to_csv(out_path, index=False)
    print(f"[UFO] Extract OK : {len(df)} lignes -> {out_path}")

    return f"[UFO] Extracted {len(df)} rows"


def clean_ufo_staging():
    """
    Étape UFO #2
    ------------
    Nettoie les données UFO :
      - parse datetime
      - supprime les lignes sans lat/lon
      - convertit duration (seconds) en numérique
      - sélectionne les colonnes utiles
    """
    import pandas as pd

    in_path = STAGING_DATA_PATH / "ufo_extracted.csv"
    out_path = STAGING_DATA_PATH / "ufo_clean.csv"

    if not in_path.exists():
        raise FileNotFoundError(f"[UFO] staging introuvable : {in_path}")

    print(f"[UFO] Nettoyage : {in_path}")
    df = pd.read_csv(in_path, on_bad_lines='skip', engine='python')

    # Parsing du datetime NUFORC
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"])

    # Lat / Lon numeriques
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude"])

    # Durée en secondes → numérique
    if "duration (seconds)" in df.columns:
        df["duration_seconds"] = pd.to_numeric(
            df["duration (seconds)"], errors="coerce"
        )
    else:
        # fallback si le nom de colonne a changé
        df["duration_seconds"] = pd.to_numeric(
            df.get("duration_seconds", 0), errors="coerce"
        )

    df["duration_seconds"] = df["duration_seconds"].fillna(0)

    # Colonnes utiles pour la suite
    keep_cols = [
        "datetime",
        "city",
        "state",
        "country",
        "shape",
        "duration_seconds",
        "comments",
        "latitude",
        "longitude",
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    df = df[keep_cols]

    df.to_csv(out_path, index=False)
    print(f"[UFO] Clean OK : {len(df)} lignes -> {out_path}")

    return f"[UFO] Cleaned {len(df)} rows"


def transform_ufo_staging():
    """
    Étape UFO #3
    ------------
    Transformations finales UFO avant enrichissement :
      - Ajout de la date (YYYY-MM-DD) pour jointure
      - Normalisation shape
      - Features sur les commentaires (length, has_comment)
    """
    import pandas as pd

    in_path = STAGING_DATA_PATH / "ufo_clean.csv"
    out_path = STAGING_DATA_PATH / "ufo_ready.csv"

    if not in_path.exists():
        raise FileNotFoundError(f"[UFO] staging clean introuvable : {in_path}")

    print(f"[UFO] Transformation : {in_path}")
    df = pd.read_csv(in_path, on_bad_lines='skip', engine='python')

    # Date pour jointure avec GSOD (en string YYYY-MM-DD)
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"])
    df["date"] = df["datetime"].dt.strftime("%Y-%m-%d")

    # Normalisation du shape
    if "shape" in df.columns:
        df["shape"] = df["shape"].fillna("unknown").str.lower().str.strip()
    else:
        df["shape"] = "unknown"

    # Commentaires
    df["comments"] = df.get("comments", "").astype(str)
    df["comment_length"] = df["comments"].str.len()
    df["has_comment"] = df["comment_length"] > 0

    df.to_csv(out_path, index=False)
    print(f"[UFO] Transform OK : {len(df)} lignes -> {out_path}")

    return "[UFO] Transform complete"


# ===================================================================
# ==========================  GSOD PIPELINE  ========================
# ===================================================================

def _decode_frshtt(code: str):
    """
    Décodage FRSHTT NOAA :
    F R S H T T  (Fog, Rain, Snow, Hail, Thunder, Tornado)
    """
    code = str(code) if not pd.isna(code) else "000000"
    code = code.zfill(6)
    return {
        "fog": code[0] == "1",
        "rain": code[1] == "1",
        "snow": code[2] == "1",
        "hail": code[3] == "1",
        "thunder": code[4] == "1",
        "tornado": code[5] == "1",
    }


def extract_gsod_landing():
    """
    Étape GSOD #1 (ultra simple, ultra rapide)
    ------------------------------------------
    Copie le fichier GSOD fusionné RAW → staging sans passer par pandas :
      raw/gsod/gsod_merged.csv -> staging/gsod_extracted.csv
    """
    import shutil

    raw_path = RAW_DATA_PATH / "gsod" / "gsod_merged.csv"
    out_path = STAGING_DATA_PATH / "gsod_extracted.csv"

    if not raw_path.exists():
        raise FileNotFoundError(f"[GSOD] Fichier RAW introuvable : {raw_path}")

    print(f"[GSOD] Extraction depuis RAW (copie simple) : {raw_path}")
    shutil.copy2(raw_path, out_path)
    print(f"[GSOD] Extract OK -> {out_path}")

    return f"[GSOD] Extracted (copied) to {out_path}"


def clean_gsod_staging():
    """
    Clean GSOD using Polars (streaming + lazy execution)
    Produces:
      - gsod_ready.csv (weather per station × day)
      - stations_ready.csv (station metadata)
    """

    import polars as pl
    import numpy as np

    in_path = STAGING_DATA_PATH / "gsod_extracted.csv"
    out_weather = STAGING_DATA_PATH / "gsod_ready.csv"
    out_stations = STAGING_DATA_PATH / "stations_ready.csv"

    if not in_path.exists():
        raise FileNotFoundError(f"[GSOD] File not found: {in_path}")

    # Remove old outputs
    for p in [out_weather, out_stations]:
        if p.exists():
            p.unlink()

    print(f"[GSOD] Cleaning with Polars (streaming) → {in_path}")

    # ================================================================
    # 1) READ CSV IN STREAMING MODE
    # ================================================================
    df = (
        pl.scan_csv(str(in_path))
        .with_columns([
            # Convert DATE
            pl.col("DATE").str.strptime(pl.Date, strict=False),
        ])
        .drop_nulls("DATE")
    )

    # ================================================================
    # 2) DECODE FRSHTT
    # ================================================================
    df = df.with_columns([
        pl.col("FRSHTT").cast(pl.Utf8).fill_null("000000").str.zfill(6).alias("FRSHTT6")
    ])

    # Vectorized booleans
    df = df.with_columns([
        (pl.col("FRSHTT6").str.slice(0,1) == "1").alias("fog"),
        (pl.col("FRSHTT6").str.slice(1,2) == "1").alias("rain"),
        (pl.col("FRSHTT6").str.slice(2,3) == "1").alias("snow"),
        (pl.col("FRSHTT6").str.slice(3,4) == "1").alias("hail"),
        (pl.col("FRSHTT6").str.slice(4,5) == "1").alias("thunder"),
        (pl.col("FRSHTT6").str.slice(5,6) == "1").alias("tornado"),
    ])

    df = df.with_columns([
        (~(pl.col("fog") | pl.col("rain") | pl.col("snow") |
           pl.col("hail") | pl.col("thunder") | pl.col("tornado"))).alias("blue_sky")
    ])

    # ================================================================
    # 3) CLEAN NUMERIC COLUMNS
    # ================================================================
    numeric_cols = ["TEMP", "DEWP", "VISIB", "WDSP", "MXSPD", "PRCP", "SNDP"]

    df = df.with_columns([
        pl.col(col).cast(pl.Float64, strict=False).alias(col)
        for col in numeric_cols if col in df.columns
    ])

    # Sentinel removal (>9000)
    df = df.with_columns([
        pl.when(pl.col(col) > 9000).then(None).otherwise(pl.col(col)).alias(col)
        for col in numeric_cols if col in df.columns
    ])

    # Write date as string
    df = df.with_columns([
        pl.col("DATE").dt.strftime("%Y-%m-%d").alias("date")
    ])

    # ================================================================
    # 4) WEATHER TABLE (station × day)
    # ================================================================
    weather_cols = [
        "STATION", "DATE", "date", "TEMP", "VISIB", "PRCP", "SNDP",
        "fog", "rain", "snow", "hail", "thunder", "tornado", "blue_sky",
    ]

    weather_cols = [c for c in weather_cols if c in df.columns]

    print("[GSOD] → Generating gsod_ready.csv (streaming)...")

    (
        df.select(weather_cols)
        .sink_csv(str(out_weather), has_header=True)
    )

    # ================================================================
    # 5) STATION METADATA
    # ================================================================
    station_cols = ["STATION", "NAME", "LATITUDE", "LONGITUDE", "ELEVATION"]
    station_cols = [c for c in station_cols if c in df.columns]

    print("[GSOD] → Generating stations_ready.csv (streaming)...")

    (
        df.select(station_cols)
        .unique(subset=["STATION"])
        .sink_csv(str(out_stations), has_header=True)
    )

    print("[GSOD] Cleaning OK ✔")
    return "[GSOD] Cleaned successfully with Polars"



def transform_gsod_staging():
    """
    Étape GSOD #3
    -------------
    Dans la version optimisée, toute la transformation utile a déjà été faite
    dans clean_gsod_staging(), qui a produit :

      - staging/gsod_ready.csv
      - staging/stations_ready.csv

    Ici, on ne fait donc plus rien, on garde la tâche pour la lisibilité du DAG.
    """
    print("[GSOD] transform_gsod_staging : no-op (déjà fait dans clean_gsod_staging)")
    return "[GSOD] transform_gsod_staging skipped (already transformed)"


# ===================================================================
# =====================  ENRICHMENT  (UFO + WEATHER)  ===============
# ===================================================================

def enrich_ufo_with_weather():
    """
    Étape #4 – Version Polars Ultra-optimisée + BallTree
    ----------------------------------------------------
    1) Lecture UFO, météo et stations via Polars
    2) Calcul nearest station via BallTree (beaucoup plus rapide que Haversine Python)
    3) Join spatio-temporel via Polars (station_id + date)
    4) Sortie : staging/ufo_enriched.csv
    """

    import polars as pl
    import numpy as np
    from sklearn.neighbors import BallTree

    ufo_path = STAGING_DATA_PATH / "ufo_ready.csv"
    gsod_path = STAGING_DATA_PATH / "gsod_ready.csv"
    stations_path = STAGING_DATA_PATH / "stations_ready.csv"
    out_path = STAGING_DATA_PATH / "ufo_enriched.csv"

    if not ufo_path.exists():
        raise FileNotFoundError(f"[ENRICH] ufo_ready introuvable : {ufo_path}")
    if not gsod_path.exists():
        raise FileNotFoundError(f"[ENRICH] gsod_ready introuvable : {gsod_path}")
    if not stations_path.exists():
        raise FileNotFoundError(f"[ENRICH] stations_ready introuvable : {stations_path}")

    print("[ENRICH] Lecture Polars…")

    # Lecture Polars
    ufo = pl.read_csv(ufo_path)
    daily = pl.read_csv(gsod_path)
    stations = pl.read_csv(stations_path)

    # ===========================================
    # 1️⃣ BallTree pour nearest-station
    # ===========================================

    stations_clean = stations.drop_nulls(["LATITUDE", "LONGITUDE"])

    # Conversion → radians
    station_coords = np.radians(
        stations_clean[["LATITUDE", "LONGITUDE"]].to_numpy()
    )

    print(f"[ENRICH] Stations valides : {len(station_coords)}")

    # BallTree haversine
    tree = BallTree(station_coords, metric="haversine")

    # UFO coords
    ufo_coords = np.radians(
        ufo[["latitude", "longitude"]].to_numpy()
    )

    print("[ENRICH] Calcul nearest station via BallTree…")

    dist, idx = tree.query(ufo_coords, k=1)  # nearest neighbor

    nearest_ids = stations_clean["STATION"].to_numpy()[idx.flatten()]

    # Ajout station_id dans UFO
    ufo = ufo.with_columns([
        pl.Series("station_id", nearest_ids)
    ])

    # ===========================================
    # 2️⃣ Join météo : station_id + date
    # ===========================================

    print("[ENRICH] Jointure UFO + météo…")

    daily = daily.rename({"STATION": "station_id"})

    enriched = (
        ufo.join(
            daily,
            on=["station_id", "date"],
            how="left",
        )
    )

    print(f"[ENRICH] OK : {enriched.height} lignes")

    enriched.write_csv(out_path)
    return "[ENRICH] UFO enriched (Polars + BallTree)"


# ===================================================================
# ============================ STAR SCHEMA ===========================
# ===================================================================
def build_star_schema_csv():
    """
    Build star schema using Polars (fast, vectorized, low memory)
    Produces:
      dim_date.csv
      dim_location.csv
      dim_shape.csv
      dim_weather_station.csv
      dim_frshtt.csv
      fact_ufo_observation.csv
      ufo_comments_raw.csv
    """
    import polars as pl

    enriched_path = STAGING_DATA_PATH / "ufo_enriched.csv"
    stations_path = STAGING_DATA_PATH / "stations_ready.csv"

    if not enriched_path.exists():
        raise FileNotFoundError(f"[STAR] ufo_enriched introuvable : {enriched_path}")
    if not stations_path.exists():
        raise FileNotFoundError(f"[STAR] stations_ready introuvable : {stations_path}")

    print("[STAR] Lecture Polars…")
    df = pl.read_csv(enriched_path)
    stations = pl.read_csv(stations_path)

    CUR = CURATED_DATA_PATH

    # ===========================
    # 1️⃣ DIM DATE
    # ===========================
    print("[STAR] DimDate…")

    # Construire la dimension date à partir de df["date"]
    dim_date = (
        df.select(pl.col("date").cast(str).unique())
        .drop_nulls()
        .with_columns([
            pl.col("date").str.strptime(pl.Date, strict=False).alias("full_date")
        ])
        .drop("date")
        .drop_nulls("full_date")
        .with_columns([
            pl.col("full_date").dt.year().alias("year"),
            pl.col("full_date").dt.month().alias("month"),
            pl.col("full_date").dt.day().alias("day"),
            pl.col("full_date").dt.weekday().alias("day_of_week"),
            pl.col("full_date").dt.quarter().alias("quarter"),
        ])
        .with_columns([
            (pl.col("day_of_week") >= 5).alias("is_weekend"),
        ])
        .with_columns([
            pl.when(pl.col("month").is_in([12, 1, 2])).then(pl.lit("winter"))
            .when(pl.col("month").is_in([3, 4, 5])).then(pl.lit("spring"))
            .when(pl.col("month").is_in([6, 7, 8])).then(pl.lit("summer"))
            .otherwise(pl.lit("autumn"))
            .alias("season")
        ])
        .with_row_count("date_key", offset=1)
    )

    dim_date.write_csv(CUR / "dim_date.csv")

    # -- Jointure correcte sur full_date --
    df = df.with_columns([
        pl.col("date").str.strptime(pl.Date, strict=False).alias("full_date")
    ])

    df = df.join(
        dim_date.select(["full_date", "date_key"]),
        on="full_date",
        how="left"
    )

    df = df.drop("full_date")

    # ===========================
    # 2️⃣ DIM LOCATION
    # ===========================
    print("[STAR] DimLocation…")

    # S'assurer que les colonnes existent
    for col in ["city", "state", "country"]:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    dim_loc = (
        df.select(["city", "state", "country", "latitude", "longitude"])
        .unique()
        .with_row_count("location_key", offset=1)
    )

    dim_loc.write_csv(CUR / "dim_location.csv")

    df = df.join(
        dim_loc,
        on=["city", "state", "country", "latitude", "longitude"],
        how="left",
    )

    # ===========================
    # 3️⃣ DIM SHAPE
    # ===========================
    print("[STAR] DimShape…")

    if "shape" not in df.columns:
        df = df.with_columns(pl.lit("unknown").alias("shape"))

    dim_shape = (
        df.select(["shape"])
        .unique()
        .with_columns([
            pl.col("shape").fill_null("unknown"),
            pl.col("shape").alias("shape_category"),
        ])
        .with_row_count("shape_key", offset=1)
    )

    dim_shape.write_csv(CUR / "dim_shape.csv")

    df = df.join(dim_shape.select(["shape", "shape_key"]), on="shape", how="left")

    # ===========================
    # 4️⃣ DIM WEATHER STATION
    # ===========================
    print("[STAR] DimWeatherStation…")

    dim_station = (
        stations.rename(
            {
                "STATION": "station_id",
                "NAME": "station_name",
                "LATITUDE": "station_latitude",
                "LONGITUDE": "station_longitude",
                "ELEVATION": "station_elevation",
            }
        )
        .unique("station_id")
        .with_row_count("station_key", offset=1)
    )

    dim_station.write_csv(CUR / "dim_weather_station.csv")

    df = df.join(
        dim_station.select(["station_id", "station_key"]),
        on="station_id",
        how="left",
    )

    # ===========================
    # 5️⃣ DIM FRSHTT
    # ===========================
    print("[STAR] DimFRSHTT…")

    base_frs_cols = ["fog", "rain", "snow", "hail", "thunder", "tornado"]

    # s'assurer que les colonnes existent et sont booléennes
    for c in base_frs_cols:
        if c not in df.columns:
            df = df.with_columns(pl.lit(False).alias(c))
        else:
            df = df.with_columns(pl.col(c).fill_null(False).cast(pl.Boolean))

    # recalculer blue_sky = aucun phénomène météo
    df = df.with_columns(
        (~pl.any_horizontal([pl.col(c) for c in base_frs_cols])).alias("blue_sky")
    )

    frs_cols_all = base_frs_cols + ["blue_sky"]

    # DIM FRSHTT unique
    dim_frshtt = (
        df.select(frs_cols_all)
        .unique()
        .with_row_count("frshtt_key", offset=1)
    )

    # label lisible : "clear / blue sky" ou "fog+rain+snow"
    dim_frshtt = dim_frshtt.with_columns(
        pl.when(pl.col("blue_sky") == True)
        .then(pl.lit("clear / blue sky"))
        .otherwise(
            pl.concat_str(
                [
                    pl.when(pl.col(c) == True)
                    .then(pl.lit(c))
                    .otherwise(pl.lit(None))
                    for c in base_frs_cols
                ],
                separator="+",
            )
        )
        .alias("label")
    )

    dim_frshtt.write_csv(CUR / "dim_frshtt.csv")

    df = df.join(
        dim_frshtt.select(frs_cols_all + ["frshtt_key"]),
        on=frs_cols_all,
        how="left",
    )

    # ===========================
    # 6️⃣ FACT UFO OBSERVATION
    # ===========================
    print("[STAR] FactUfoObservation…")

    needed_fact_cols = [
        "date_key",
        "location_key",
        "shape_key",
        "station_key",
        "frshtt_key",
        "duration_seconds",
        "TEMP",
        "VISIB",
        "comment_length",
        "has_comment",
    ]

    # créer colonnes manquantes si besoin
    for col in ["duration_seconds", "TEMP", "VISIB", "comment_length"]:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))
    if "has_comment" not in df.columns:
        df = df.with_columns(pl.lit(False).alias("has_comment"))

    fact = (
        df.select(needed_fact_cols)
        .rename(
            {
                "TEMP": "temp_mean",
                "VISIB": "visibility_mean",
            }
        )
        .with_row_count("fact_id", offset=1)
    )

    fact = fact.select(
        [
            "fact_id",
            "date_key",
            "location_key",
            "shape_key",
            "station_key",
            "frshtt_key",
            "duration_seconds",
            "temp_mean",
            "visibility_mean",
            "comment_length",
            "has_comment",
        ]
    )

    fact.write_csv(CUR / "fact_ufo_observation.csv")

    # ===========================
    # 7️⃣ UFO COMMENTS RAW
    # ===========================
    print("[STAR] UfoCommentsRaw…")

    if "comments" in df.columns:
        comments_series = df["comments"]
    else:
        comments_series = pl.Series([""] * df.height)

    comments = pl.DataFrame(
        {
            "fact_id": fact["fact_id"],
            "comment_text": comments_series,
        }
    )

    comments.write_csv(CUR / "ufo_comments_raw.csv")

    print("[STAR] Star schema généré ✔")
    return "[STAR] OK"
