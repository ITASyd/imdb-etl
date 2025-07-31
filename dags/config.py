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
VISUALIZATIONS = BASE_PATH / "visualizations"

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


# _______ DOC MD ________ #

DAG_MD = """
        # IMDB DAG

        This DAG extracts the dataset from the IMDB website and performs some example operations.

        - Data extractions
        - Decompression and conversion in csv
        - Basic transformation
        - Loading data into SQL database
        """

EXT_MD = """
        # Extraction

        This task downloads the dataset from IMDB website and decompresses it.
        """

TRA_MD = """
        # Transformation

        This task uses Pandas to create DataFrames from the TSV files, then:
        1. Filters only 'movie' titles
        2. Joins ratings
        3. Computes:
        * film count per genre
        * top-rated films per genre (votes ≥ 10k)
        * saves CSV files in data/processed/.
        """

LOA_MD = """
        # Loading
        
        This task loads the transformed data inside a PostgreSQL database.
        """

ANA_MD = """
        # Analysis

        This task analyzes film genre data from a database, logging top genres by average rating and the most voted film per genre.
        This function connects to a database, retrieves film data, and performs two main analyses:
        1. Finds and logs the top 3 genres with the highest average ratings (considering only genres with at least 100 movies) using an SQL query.
        2. Identifies and logs the most voted film for each genre using Pandas operations.
        """