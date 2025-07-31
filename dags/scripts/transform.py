import pandas as pd
import pandera as pa
import time
import os
from pandera.typing import Series
from pandera.errors import SchemaError
from airflow.utils.log.logging_mixin import LoggingMixin
from dags.config import (PROCESSED,
                         BASICS_TSV,
                         RATINGS_TSV,
                         MIN_VOTES,
                         SEPARATOR,
                         MISSING_VALUE,
                         GENRE_CSV,
                         BEST_CSV)

class ExplodedSchema(pa.DataFrameModel):
    tconst: str
    primaryTitle: str
    startYear: Series[int]
    genres: str
    averageRating: Series[float] = pa.Field(ge=0.0, le=10.0)
    numVotes: Series[int] = pa.Field(ge=0)

    class Config:
        strict = True

def transform ():
    """
    Transforms IMDb data:
    1. Filters only 'movie' titles
    2. Joins ratings
    3. Computes:
       • film count per genre
       • top-rated films per genre (votes ≥ 10k)
    Saves CSV files in data/processed/.
    """
    start = time.time()
    log = LoggingMixin().log

    
    PROCESSED.mkdir(parents=True, exist_ok=True)

    try:
        # File reading
        basics = pd.read_csv(BASICS_TSV, sep=SEPARATOR, na_values=MISSING_VALUE)
        ratings = pd.read_csv(RATINGS_TSV, sep=SEPARATOR, na_values=MISSING_VALUE)
    except FileNotFoundError as e:
        log.error(f"File not found: {e}")
        return
    except pd.errors.ParserError as e:
        log.error(f"Parsing error: {e}")
        return
    
    
    # Filtering
    movies = basics[basics["titleType"]=="movie"].copy()
    movies = movies[["tconst", "primaryTitle", "startYear", "genres"]].dropna(subset=['primaryTitle','genres'])

    # Joining using tconst as key
    ratings_subset = ratings[["tconst", "averageRating", "numVotes"]]
    joined = pd.merge(movies, ratings_subset, on="tconst", how="left")
    joined["genres"] = joined["genres"].str.split(",")
    exploded = joined.explode("genres")


    # Conversione tipi e controllo nulli prima della validazione Pandera
    exploded['startYear'] = pd.to_numeric(exploded['startYear'], errors='coerce')
    exploded['averageRating'] = pd.to_numeric(exploded['averageRating'], errors='coerce')
    exploded['numVotes'] = pd.to_numeric(exploded['numVotes'], errors='coerce')
    null_rows = exploded[exploded.isnull().any(axis=1)]
    if not null_rows.empty:
        log.warning(f"Rows with nulls before validation:\n{null_rows.head()}")
    exploded = exploded.dropna(subset=['tconst', 'primaryTitle', 'startYear', 'genres', 'averageRating', 'numVotes'])

    # Pandera validation
    try:
        ExplodedSchema.validate(exploded)
    except SchemaError as err:
        log.error("Error during Pandera validation:")
        log.error(err.failure_cases)
        pass

    # Count by genre
    genre_counts = exploded.groupby("genres")["tconst"].count().reset_index()
    genre_counts.columns = ["genre", "film_count"]
    genre_counts.sort_values(by="film_count", ascending=False)
    genre_counts.to_csv(GENRE_CSV, index=False)

    # Best movie by genre
    top = (exploded[exploded["numVotes"] >= MIN_VOTES]
            .sort_values(["genres", "averageRating", "numVotes"], ascending=[True, False, False])
            .groupby("genres")
            .first()
            .reset_index()
        )

    top = top[["genres", "primaryTitle", "startYear", "averageRating", "numVotes"]]
    top.columns = ["genre", "title", "startYear", "rating", "votes"]
    top.to_csv(BEST_CSV, index=False)

    # Data for metrics
    duration = time.time() - start
    duration = f"{duration:.2f}"
    files_size = (os.path.getsize(GENRE_CSV)) + (os.path.getsize(BEST_CSV))
    log.info(f"duration: {duration}, file size: {files_size}")
    return {
        "duration": duration,
        "files_size": files_size
    }