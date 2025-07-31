from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pathlib import Path
from scripts.extract import extract
from scripts.transform import transform
from scripts.load import load
from scripts.analyze import analyze
from scripts.visualize.transform_visual import transform_visualize
from scripts.visualize.extract_visual import extract_visualize
from scripts.visualize.analyze_visual import analyze_visualize
from datetime import timedelta
from config import (DAG_MD,
                    EXT_MD,
                    TRA_MD,
                    LOA_MD,
                    ANA_MD)

with DAG (
    dag_id="imdb_pipeline",
    start_date=datetime(2024,1,1),
    schedule_interval="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["etl"],
    doc_md=DAG_MD
) as dag:
    
    t1 = PythonOperator(task_id="extract",
                        python_callable=extract,
                        retry_delay=timedelta(minutes=5),
                        execution_timeout=timedelta(minutes=30),
                        doc_md=EXT_MD)
    t2 = PythonOperator(task_id="transform",
                        python_callable=transform,
                        retry_delay=timedelta(minutes=5),
                        execution_timeout=timedelta(minutes=30),
                        doc_md=TRA_MD)
    t3 = PythonOperator(task_id="load",
                        python_callable=load,
                        retry_delay=timedelta(minutes=5),
                        execution_timeout=timedelta(minutes=30),
                        doc_md=LOA_MD)
    t4 = PythonOperator(task_id="analyze",
                        python_callable=analyze,
                        retry_delay=timedelta(minutes=5),
                        execution_timeout=timedelta(minutes=30),
                        doc_md=ANA_MD)
    
    v1 = PythonOperator(task_id="extract_visualize",
                        python_callable=extract_visualize)
    
    v2 = PythonOperator(task_id="transform_visualize",
                        python_callable=transform_visualize)
    
    v3 = PythonOperator(task_id="analyze_visualize",
                        python_callable=analyze_visualize)

    t1 >> [t2, v1]
    t2 >> [t3, v2]
    t3 >> t4
    t4 >> v3