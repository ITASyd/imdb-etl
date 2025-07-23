from sqlalchemy import create_engine
import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin
from dags.config import BEST_CSV, GENRE_CSV, DB_CONNECTION_STRING

def load():
    """
    Load the transformed data inside a PostgreSQL database.
    """
    
    log = LoggingMixin().log
    try:
        engine = create_engine(DB_CONNECTION_STRING)

        genre = pd.read_csv(GENRE_CSV)
        best = pd.read_csv(BEST_CSV)

        genre.to_sql("films_for_genre", con=engine, if_exists="replace", index=False)
        best.to_sql("best_films_per_genre", con=engine, if_exists="replace", index=False)

        read_genre = pd.read_sql("SELECT *  FROM films_for_genre LIMIT 5", con=engine)
        read_best = pd.read_sql("SELECT *  FROM best_films_per_genre LIMIT 5", con=engine)
        
        log.info(read_best)
        log.info(read_genre)
        log.info("Data loaded successfully.")
    
    except Exception as e:
        log.error(f"Error during loading: {e}")
        raise
