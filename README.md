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
│   ├── imdb_pipeline.py         # Airflow DAG definition
│   ├── scripts/
│   │   ├── extract.py          # Extract task (download + decompress)
│   │   ├── transform.py        # Transform task (cleaning + aggregations)
│   │   ├── load.py             # Load task (PostgreSQL insert)
│   │   ├── metrics.py          # Generate metrics for the DAG (collect via xcom)
│   │   └── visualize/          # Create charts with metrics (matplotlib)
│   │       ├── extract_visual.py
│   │       ├── transform_visual.py
│   │       ├── load_visual.py
│   │       └── metrics_visual.py
│   └── tests/
│       ├── test_extract.py     # Unit tests for extract
│       ├── test_transform.py   # Unit tests for transform
│       ├── test_load.py        # Unit tests for load
│       └── test_analyze_visual.py # Unit tests for visualization
├── data/
│   ├── raw/                    # Raw IMDb data (.tsv)
│   ├── processed/              # Processed output (.csv)
│   └── visualizations/         # Generated charts (.png)
├── docker-compose.yaml         # Airflow + Postgres orchestration
├── .env                        # Environment variables (not committed)
├── requirements.txt            # Python dependencies
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
- Cleans and validates data (Pandera)
- Computes:
  - Movie count per genre
  - Best-rated movies per genre (with ≥ 10,000 votes)
- Saves two processed CSV files:
  - `/data/processed/films_for_genre.csv`
  - `/data/processed/best_films_per_genre.csv`
- Returns metrics (duration, file size) via XCom for downstream tasks

### 3. Load
- Loads processed data into PostgreSQL via SQLAlchemy
- Receives file paths via XCom from the transform task
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

best_films_per_genre.csv
---

## DAG Data Flow & Metrics

- Data between tasks is passed using Airflow XCom (not via intermediate files)
- The transform task returns a dict with:
  - `duration`: seconds taken for transformation
  - `files_size`: total size of processed CSV files
- The load task reads processed file paths from XCom and loads them into PostgreSQL
- Analyze and visualize tasks use XCom to pass results and metrics for reporting

---

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
    matplotlib
    pytest
    apache-airflow (installed via Docker)

---

## Data Visualization

The pipeline includes visualization tasks using **matplotlib**:

- Generates summary charts (e.g., most voted movies, genre distributions)
- Visualizations are saved as PNG files in `/data/visualizations/`
- Example: `analyze_visual.py` creates a horizontal bar chart of top movies by votes

---

## Testing

Unit tests are provided for all main ETL tasks using **pytest**:

- Test scripts are in `dags/tests/`
- Each ETL function has a corresponding test that mocks dependencies and checks outputs
- Example: `test_transform.py` verifies the transformation logic and output metrics
- To run all tests:
```
pytest dags/tests/
```

---

Author

    Diego Cinelli — @ITASyd