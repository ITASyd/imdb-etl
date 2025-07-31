import pandas as pd
from pathlib import Path
from airflow.utils.log.logging_mixin import LoggingMixin
from config import METRICS

def collect_metrics(**context):
    """
    Collects ETL process metrics from Airflow XComs, aggregates them into a DataFrame,
    and appends or writes the results to a CSV file for tracking pipeline performance.    
    """

    log = LoggingMixin().log
    METRICS.mkdir(parents=True, exist_ok=True)
    metrics_csv = Path(METRICS / "etl_metrics.csv")

    try:
        # Retrieving data
        data_extract = context["ti"].xcom_pull(task_ids="extract")
        start_date = data_extract["start_date"]
        extract_duration = data_extract["duration"]
        extract_rows = data_extract["row_count"]
        extract_size = data_extract["files_size"]
        data_transform = context["ti"].xcom_pull(task_ids="transform")
        transform_duration = data_transform["duration"]
        transform_size = data_transform["files_size"]
        data_load = context["ti"].xcom_pull(task_ids="load")
        load_duration = data_load["duration"]
        data_analyze = context["ti"].xcom_pull(task_ids="analyze")
        analyze_duration = data_analyze["duration"]

        # Building DataFrame
        data = [{
            "start": start_date,
            "extract(seconds)": extract_duration,
            "extract(rows)": extract_rows,
            "extract(bytes)": extract_size,
            "transform(seconds)": transform_duration,
            "transform(bytes)": transform_size,
            "load(seconds)": load_duration,
            "analyze(seconds)": analyze_duration
        }]
        dag_df = pd.DataFrame(data)

        # Creating or updating data
        if metrics_csv.exists():
            existing = pd.read_csv(metrics_csv)
            updated = pd.concat([existing, dag_df], ignore_index=True)
        else:
            updated = dag_df
        updated.to_csv(metrics_csv, index=False)
        log.info(f"Metrics updated: {metrics_csv} ({len(updated)} rows)")

    except Exception as e:
        log.error(f"Error collecting metrics: {e}")
        raise