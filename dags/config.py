import os
from pathlib import Path
from dotenv import load_dotenv

# _______ UNIVERSAL VARIABLES_______ #

load_dotenv(".env")
# Detect environment
IS_DOCKER = os.getenv("AIRFLOW_EXECUTION_ENV", "local") == "docker"
# Base paths
BASE_PATH = Path("/opt/airflow/data") if IS_DOCKER else Path("data")

RAW = BASE_PATH / "raw"
PROCESSED = BASE_PATH / "processed"

# DB Connection variables
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

DB_CONNECTION_STRING = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# _______ EXTRACT VARIABLES _______ #
FILES = {
        "title.basics.tsv.gz": "https://datasets.imdbws.com/title.basics.tsv.gz",
        "title.ratings.tsv.gz": "https://datasets.imdbws.com/title.ratings.tsv.gz",
        "name.basics.tsv.gz": "https://datasets.imdbws.com/name.basics.tsv.gz"
    }

# ______ TRANSFORM VARIABLES _______ #
BASICS_TSV = RAW / "title.basics.tsv"
RATINGS_TSV = RAW / "title.ratings.tsv"

# Transformation parameters
MIN_VOTES = 10_000
SEPARATOR = "\t"
MISSING_VALUE = "\\N"

# _______ TRANSFORM AND LOAD VARIABLES _______ #
GENRE_CSV = PROCESSED / "films_for_genre.csv"
BEST_CSV = PROCESSED / "best_films_per_genre.csv"