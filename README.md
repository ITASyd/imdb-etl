# IMDb ETL Pipeline with Airflow & PostgreSQL

## Overview

This project implements an **ETL pipeline** using **Apache Airflow**, **Python**, and **PostgreSQL** to extract, transform, and load datasets from [IMDb](https://www.imdb.com/interfaces/).  
It is structured as a production-ready project with modular scripts and DAGs.

> **Goal**: Automate the ingestion and transformation of IMDb metadata (titles, ratings, and names), and load results into a PostgreSQL database.

---

## Project Structure

```
imdb-etl/
├── dags/
│ ├── imdb_pipeline.py # Airflow DAG definition
│ └── scripts/
│ ├── extract.py # Extract task (download + decompress)
│ ├── transform.py # Transform task (cleaning + aggregations)
│ └── load.py # Load task (PostgreSQL insert)
├── data/
│ ├── raw/ # Raw IMDb data (.tsv)
│ └── processed/ # Processed output (.csv)
├── docker-compose.yaml # Airflow + Postgres orchestration
├── .env # Environment variables (not committed)
├── requirements.txt # Python dependencies
└── README.md
```


---

## Datasets Used

IMDb public datasets (updated daily), including:

- `title.basics.tsv.gz`: Basic info (title, genre, year)
- `title.ratings.tsv.gz`: User ratings and vote count
- `name.basics.tsv.gz`: Names and professions (not used in this pipeline yet)

---

## ETL Pipeline Steps

### 1. Extract
- Downloads and decompresses `.gz` files from IMDb URLs
- Saves raw `.tsv` files in `/data/raw/`

### 2. Transform
- Filters titles of type `movie`
- Joins `ratings` on `tconst`
- Computes:
  - Movie count per genre
  - Best-rated movies per genre (with ≥ 10,000 votes)
- Saves two processed CSV files:
  - `/data/processed/films_for_genre.csv`
  - `/data/processed/best_films_per_genre.csv`

### 3. Load
- Loads processed data into PostgreSQL via SQLAlchemy
- Creates or replaces the following tables:
  - `films_for_genre`
  - `best_films_per_genre`

---

## Setup Instructions

### 1. Clone the repository
```
bash
git clone https://github.com/yourusername/imdb-etl.git
cd imdb-etl
```

2. Configure environment

Create a .env file based on .env.example and fill in your PostgreSQL credentials.

This is used by both Airflow and your load.py script.

Create a docker-compose.yaml file based on docker-compose.yaml.example and fill in you credentials under 'airflow-init'.

3. Run via Docker

Make sure Docker is installed and run:

```
docker-compose up --build
```

This will start:

    Airflow Web UI at localhost:8080

    PostgreSQL database service

Login to Airflow using the credentials in the .yaml file.

4. Trigger the DAG

Once the DAG imdb_pipeline appears in the UI:

    Unpause it

    Trigger a manual run

    Monitor task progress in the graph view

Example Output

```
films_for_genre.csv
genre	film_count
Drama	18921
Comedy	14430
Horror	6721

best_films_per_genre.csv
genre	title	startYear	rating	votes
Drama	The Shawshank...	1994	9.3	2500000
Comedy	Life Is Beautiful	1997	8.6	800000
```

Dependencies

Install them with:
```
pip install -r requirements.txt
```

Main libraries:

    pandas, requests, urllib3

    sqlalchemy, psycopg2-binary

    python-dotenv

    apache-airflow (installed via Docker)

Author

    Diego Cinelli — @ITASyd