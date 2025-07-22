from sqlalchemy import create_engine
from dotenv import load_dotenv
from urllib.parse import quote_plus
import os
import pandas as pd
from dags.config import BEST_CSV, GENRE_CSV, DB_CONNECTION_STRING

def load():

    engine = create_engine(DB_CONNECTION_STRING)

    genre = pd.read_csv(GENRE_CSV)
    best = pd.read_csv(BEST_CSV)

    genre.to_sql("films_for_genre", con=engine, if_exists="replace", index=False)
    best.to_sql("best_films_per_genre", con=engine, if_exists="replace", index=False)

    read_genre = pd.read_sql("SELECT *  FROM films_for_genre LIMIT 5", con=engine)
    read_best = pd.read_sql("SELECT *  FROM best_films_per_genre LIMIT 5", con=engine)
    print(read_genre)
    print(read_best)
    print("Operazione eseguita con successo.")
