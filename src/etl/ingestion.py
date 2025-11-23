# src/etl/ingestion.py

import os
from pathlib import Path
from zipfile import ZipFile

BASE_DATA_PATH = Path(os.getenv("DATA_PATH", "/opt/airflow/data"))
RAW_DATA_PATH = Path(os.getenv("RAW_DATA_PATH", "/opt/airflow/data/raw"))

UFO_RAW_DIR = RAW_DATA_PATH / "ufo"
GSOD_RAW_DIR = RAW_DATA_PATH / "gsod"

UFO_CSV_PATH = UFO_RAW_DIR / "complete.csv"


def get_mongo_client():
    # import interne (léger)
    from pymongo import MongoClient

    user = os.getenv("MONGO_INITDB_ROOT_USERNAME", "mongoadmin")
    pwd = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "mongopwd")
    host = os.getenv("MONGO_HOST", "mongo")
    port = int(os.getenv("MONGO_PORT", "27017"))
    uri = f"mongodb://{user}:{pwd}@{host}:{port}/"
    return MongoClient(uri)


def init_dirs():
    UFO_RAW_DIR.mkdir(parents=True, exist_ok=True)
    GSOD_RAW_DIR.mkdir(parents=True, exist_ok=True)


# =====================================
#            DOWNLOAD UFO
# =====================================

def get_kaggle_api():
    """
    Initialise le client Kaggle. Import interne pour éviter le timeout Airflow.
    """
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()
    return api


# def download_ufo():
#     """
#     Télécharge le dataset UFO depuis Kaggle.
#     """
#     import pandas as pd  # léger, mais mieux en interne

#     init_dirs()
#     api = get_kaggle_api()

#     tmp_dir = RAW_DATA_PATH / "tmp_ufo"
#     tmp_dir.mkdir(parents=True, exist_ok=True)

#     api.dataset_download_files(
#         "NUFORC/ufo-sightings",
#         path=str(tmp_dir),
#         unzip=True
#     )

#     src = tmp_dir / "complete.csv"
#     if not src.exists():
#         raise FileNotFoundError(f"[UFO] complete.csv introuvable dans {tmp_dir}")

#     UFO_RAW_DIR.mkdir(parents=True, exist_ok=True)
#     src.replace(UFO_CSV_PATH)

#     # Clean temp dir
#     for f in tmp_dir.iterdir():
#         f.unlink()
#     tmp_dir.rmdir()

#     return f"[UFO] Dataset téléchargé et stocké dans {UFO_CSV_PATH}"

def download_ufo(max_retries=5):
    """
    Télécharge le dataset UFO depuis Kaggle.
    """
    import time

    init_dirs()
    api = get_kaggle_api()

    tmp_dir = RAW_DATA_PATH / "tmp_ufo"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, max_retries + 1):
        try:
            api.dataset_download_files(
                "NUFORC/ufo-sightings",
                path=str(tmp_dir),
                unzip=True
            )
            break
        except Exception as e:
            print(f"[UFO] Download attempt {attempt} failed: {e}")
            time.sleep(5 * attempt)
    else:
        raise RuntimeError("UFO: all retries failed")

    src = tmp_dir / "complete.csv"
    if not src.exists():
        raise FileNotFoundError("[UFO] complete.csv introuvable après download")

    UFO_RAW_DIR.mkdir(parents=True, exist_ok=True)
    src.replace(UFO_CSV_PATH)
    return f"[UFO] Dataset téléchargé et stocké dans {UFO_CSV_PATH}"


# =====================================
#     DOWNLOAD & MERGE GSOD NOAA
# =====================================

BASE_ARCHIVE_URL = "https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive"

def download_gsod_archives():
    """
    1) Download en parallèle des archives GSOD .tar.gz (1980–2000)
    """
    import requests
    from concurrent.futures import ThreadPoolExecutor

    init_dirs()

    YEARS = range(1980, 2001)
    BASE_URL = "https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive"

    ARCHIVE_DIR = RAW_DATA_PATH / "tmp_gsod" / "archives"
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    def download_year(year):
        url = f"{BASE_URL}/{year}.tar.gz"
        dst = ARCHIVE_DIR / f"{year}.tar.gz"
        try:
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(dst, "wb") as f:
                    for chunk in r.iter_content(8192):
                        f.write(chunk)
            print(f"[GSOD] Download OK : {year}")
            return year
        except Exception as e:
            print(f"[GSOD] Download FAIL {year}: {e}")
            return None

    print("\n=== DOWNLOAD PHASE ===")
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(download_year, YEARS))

    downloaded = [y for y in results if y]
    if not downloaded:
        raise RuntimeError("Aucune archive GSOD téléchargée.")

    return downloaded

def extract_gsod_archives():
    """
    2) Extraction en parallèle des archives GSOD
    """
    import tarfile
    from concurrent.futures import ThreadPoolExecutor
    import os

    ARCHIVE_DIR = RAW_DATA_PATH / "tmp_gsod" / "archives"
    EXTRACT_DIR = RAW_DATA_PATH / "tmp_gsod" / "extracted"
    EXTRACT_DIR.mkdir(parents=True, exist_ok=True)

    archives = list(ARCHIVE_DIR.glob("*.tar.gz"))
    if not archives:
        raise RuntimeError("Aucune archive .tar.gz trouvée.")

    def extract_archive(path):
        year = path.stem  # "1980"
        target = EXTRACT_DIR / year
        target.mkdir(parents=True, exist_ok=True)
        try:
            with tarfile.open(path, "r:gz") as tar:
                tar.extractall(path=target)
            print(f"[GSOD] Extract OK : {year}")
            return year
        except Exception as e:
            print(f"[GSOD] Extract FAIL {year}: {e}")
            return None

    print("\n=== EXTRACTION PHASE ===")
    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(extract_archive, archives))

    extracted = [y for y in results if y]
    if not extracted:
        raise RuntimeError("Aucune extraction GSOD réussie.")

    return extracted

def merge_gsod_years():
    """
    Fusion GSOD super-optimisée.
    - Streaming : aucun concat, aucun DF géant en mémoire
    - Lecture de 1 CSV à la fois
    - Append direct dans un unique CSV final
    """

    import pandas as pd

    EXTRACT_DIR = RAW_DATA_PATH / "tmp_gsod" / "extracted"
    FINAL_DIR = GSOD_RAW_DIR
    FINAL_DIR.mkdir(parents=True, exist_ok=True)

    final_path = FINAL_DIR / "gsod_1980_2010.csv"

    # Écraser le fichier s'il existe
    if final_path.exists():
        final_path.unlink()

    print("\n=== FINAL MERGE (streaming, ultra rapide) ===")

    # Liste triée des années pour conserver un ordre cohérent
    years = sorted([p.name for p in EXTRACT_DIR.iterdir() if p.is_dir()])
    if not years:
        raise RuntimeError("[GSOD] Aucune année détectée dans extracted/")

    first_write = True

    for year in years:
        print(f"[GSOD] Fusion de l’année {year}...")

        year_dir = EXTRACT_DIR / year
        csv_files = list(year_dir.rglob("*.csv"))

        if not csv_files:
            print(f"[GSOD] Aucun CSV trouvé pour {year}")
            continue

        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)

                # Ajout de l'année pour traçabilité
                df["year"] = int(year)

                df.to_csv(
                    final_path,
                    mode="w" if first_write else "a",
                    header=first_write,
                    index=False
                )

                first_write = False

            except Exception as e:
                print(f"[GSOD] Erreur fichier {csv_file}: {e}")

    print(f"[GSOD] Fichier final : {final_path}")
    return str(final_path)




# =====================================
#           INSERT MONGODB
# =====================================

def insert_ufo_into_mongo():
    import pandas as pd

    if not UFO_CSV_PATH.exists():
        raise FileNotFoundError(f"Fichier UFO introuvable : {UFO_CSV_PATH}")

    df = pd.read_csv(UFO_CSV_PATH, on_bad_lines='skip', engine='python')

    client = get_mongo_client()
    coll = client["landing_db"]["ufo_raw"]

    records = df.to_dict(orient="records")
    if records:
        coll.delete_many({})
        coll.insert_many(records)

    return f"[UFO] {len(records)} documents insérés"


def insert_gsod_into_mongo():
    import pandas as pd

    merged_path = GSOD_RAW_DIR / "gsod_1980_2010.csv"

    if not merged_path.exists():
        raise FileNotFoundError(f"GSOD fusionné introuvable : {merged_path}")

    df = pd.read_csv(merged_path)

    client = get_mongo_client()
    coll = client["landing_db"]["gsod_raw"]

    records = df.to_dict(orient="records")
    if records:
        coll.delete_many({})
        coll.insert_many(records)

    return f"[GSOD] {len(records)} documents insérés"


# =====================================
#               CLEAN TMP
# =====================================

def clean_tmp_dirs():
    """
    Supprime les dossiers temporaires GSOD & UFO.
    """
    def clean(dirpath):
        if dirpath.exists():
            for child in dirpath.glob("**/*"):
                try:
                    child.unlink()
                except:
                    pass
            try:
                dirpath.rmdir()
            except:
                pass

    clean(RAW_DATA_PATH / "tmp_gsod")
    clean(RAW_DATA_PATH / "tmp_ufo")

    print("[CLEAN] OK")
    return "[CLEAN] Nettoyage terminé."
