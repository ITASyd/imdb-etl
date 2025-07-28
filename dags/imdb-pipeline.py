from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pathlib import Path
from scripts.extract import extract
from scripts.transform import transform
from scripts.load import load
from scripts.analyze import analyze

with DAG (
    dag_id="imdb_pipeline",
    start_date=datetime(2024,1,1),
    schedule_interval="0 0 * * *",
    catchup=False,
    tags=["etl"]
) as dag:
    
    t1 = PythonOperator(task_id="extract", python_callable=extract)
    t2 = PythonOperator(task_id="transform", python_callable=transform)
    t3 = PythonOperator(task_id="load", python_callable=load)
    t4 = PythonOperator(task_id="analyze", python_callable=analyze)

    t1 >> t2 >> t3 >> t4