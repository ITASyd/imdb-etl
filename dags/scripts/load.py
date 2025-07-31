from sqlalchemy import create_engine
import pandas as pd
import pandera as pa
import time
from pandera.typing import Series
from pandera.errors import SchemaError
from airflow.utils.log.logging_mixin import LoggingMixin
from dags.config import BEST_CSV, GENRE_CSV, DB_CONNECTION_STRING

class GenreSchema(pa.DataFrameModel):
    genre: str
    film_count: int

    class Config:
        strict = True

class BestSchema(pa.DataFrameModel):
    genre: str
    title: str
    startYear: float
    rating: float
    votes: float

    class Config:
        strict = True

def load():
    """
    Load the transformed data inside a PostgreSQL database.
    """
    start = time.time()
    log = LoggingMixin().log
    try:
        engine = create_engine(DB_CONNECTION_STRING)

        genre = pd.read_csv(GENRE_CSV)
        best = pd.read_csv(BEST_CSV)

        # Pandera validation
        try:
            BestSchema.validate(best)
            GenreSchema.validate(genre)
        except SchemaError as err:
            log.error("Error during Pandera validation:")
            log.error(err.failure_cases)
            return

        genre.to_sql("films_for_genre", con=engine, if_exists="replace", index=False)
        best.to_sql("best_films_per_genre", con=engine, if_exists="replace", index=False)

        read_genre = pd.read_sql("SELECT *  FROM films_for_genre LIMIT 5", con=engine)
        read_best = pd.read_sql("SELECT *  FROM best_films_per_genre LIMIT 5", con=engine)
        
        log.info(read_best)
        log.info(read_genre)
        log.info("Data loaded successfully.")
        duration = time.time() - start
        duration = f"{duration:.2f}"
        log.info(f"Duration: {duration}")
        return {"duration": duration}
    
    except Exception as e:
        log.error(f"Error during loading: {e}")
        raise
