import pandas as pd
from pathlib import Path

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
    raw = Path("/opt/airflow/data/raw") #DOCKER
    proc = Path("/opt/airflow/data/processed") #DOCKER
    proc.mkdir(parents=True, exist_ok=True)

    # File reading
    basics = pd.read_csv(raw / "title.basics.tsv", sep="\t", na_values="\\N")
    ratings = pd.read_csv(raw / "title.ratings.tsv", sep="\t", na_values="\\N")

    # Filtering
    movies = basics[basics["titleType"]=="movie"].copy()
    movies = movies[["tconst", "primaryTitle", "startYear", "genres"]].dropna(subset=['primaryTitle','genres'])

    # Joining using tconst as key
    ratings_subset = ratings[["tconst", "averageRating", "numVotes"]]
    joined = pd.merge(movies, ratings_subset, on="tconst", how="left")
    joined["genres"] = joined["genres"].str.split(",")
    exploded = joined.explode("genres")

    # Count by genre
    genre_counts = exploded.groupby("genres")["tconst"].count().reset_index()
    genre_counts.columns = ["genre", "film_count"]
    genre_counts.sort_values(by="film_count", ascending=False)
    genre_counts.to_csv(proc / "films_for_genre.csv", index=False)

    # Best movie by genre
    top = (exploded[exploded["numVotes"] >= 10000]
            .sort_values(["genres", "averageRating", "numVotes"], ascending=[True, False, False])
            .groupby("genres")
            .first()
            .reset_index()
    )

    top = top[["genres", "primaryTitle", "startYear", "averageRating", "numVotes"]]
    top.columns = ["genre", "title", "startYear", "rating", "votes"]
    top.to_csv(proc / "best_films_per_genre.csv", index=False)
