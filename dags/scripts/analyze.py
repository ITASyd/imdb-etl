import pandas as pd
from pandera.typing import Series
from pandera.errors import SchemaError
from airflow.utils.log.logging_mixin import LoggingMixin
from sqlalchemy import create_engine
from dags.config import DB_CONNECTION_STRING

def analyze():
    log = LoggingMixin().log
    
    try:
        engine = create_engine(DB_CONNECTION_STRING)
        genre_frame = pd.read_sql_table(table_name="best_films_per_genre", con=engine)

        # Top 3 genres for average ratings (with at least 100 movies), w SQL Query
        sql_query = """
        SELECT best.genre, AVG(best.rating) AS rat, genre.film_count
        FROM best_films_per_genre AS best
        JOIN films_for_genre AS genre ON best.genre = genre.genre
        WHERE genre.film_count >= 100
        GROUP BY best.genre, genre.film_count
        ORDER BY rat DESC
        LIMIT 3
        """
        sql_result = pd.read_sql_query(sql_query, con=engine)
        log.info("Top 3 genres by average rating:\n%s", sql_result)

        # Films with more votes for each genre, w Pandas
        genre_sorted = genre_frame.sort_values(by=["genre", "votes"], ascending=[True, False])
        top_movies_per_genre = genre_sorted.drop_duplicates(subset="genre", keep="first")
        top_movies_per_genre = top_movies_per_genre[["genre", "title", "votes"]]
        top_movies_per_genre = top_movies_per_genre.sort_values(by="votes", ascending=False)
        log.info("Most voted film per genre (top 5):\n%s", top_movies_per_genre.head())


    except Exception as e:
        log.error(f"Error during analysis: {e}")