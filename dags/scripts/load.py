from sqlalchemy import create_engine
from dotenv import load_dotenv
from urllib.parse import quote_plus
import os
import pandas as pd

def load():
    load_dotenv(".env")

    DB_USER = os.getenv("POSTGRES_USER")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    DB_NAME = os.getenv("POSTGRES_DB")
    DB_HOST = os.getenv("POSTGRES_HOST")
    DB_PORT = os.getenv("POSTGRES_PORT", "5432")

    connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    engine = create_engine(connection_string)

    genre = pd.read_csv("/opt/airflow/data/processed/films_for_genre.csv")
    best = pd.read_csv("/opt/airflow/data/processed/best_films_per_genre.csv")

    genre.to_sql("films_for_genre", con=engine, if_exists="replace", index=False)
    best.to_sql("best_films_per_genre", con=engine, if_exists="replace", index=False)

    read_genre = pd.read_sql("SELECT *  FROM films_for_genre LIMIT 5", con=engine)
    read_best = pd.read_sql("SELECT *  FROM best_films_per_genre LIMIT 5", con=engine)
    print(read_genre)
    print(read_best)
    print("Operazione eseguita con successo.")
