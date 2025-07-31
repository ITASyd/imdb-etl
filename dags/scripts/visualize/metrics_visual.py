import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from airflow.utils.log.logging_mixin import LoggingMixin
from config import METRICS, VISUALIZATIONS

def metrics_visualize(**context):
    """
    Reads the ETL metrics CSV, creates summary statistics and a performance plot.
    The visualization is saved as a PNG in the visualizations folder.
    """

    log = LoggingMixin().log
    metrics_csv = METRICS / "etl_metrics.csv"
    vis_dir = VISUALIZATIONS
    vis_dir.mkdir(parents=True, exist_ok=True)

    try:
        if not metrics_csv.exists():
            log.warning("No metrics found to visualize.")
            return

        # Load the metrics
        df = pd.read_csv(metrics_csv)
        log.info(f"Loaded {len(df)} rows from metrics file.")

        # Compute summary statistics
        avg_extract = df["extract(seconds)"].mean()
        avg_transform = df["transform(seconds)"].mean()
        avg_load = df["load(seconds)"].mean()
        log.info(f"Average durations - Extract: {avg_extract:.2f}s, Transform: {avg_transform:.2f}s, Load: {avg_load:.2f}s")

        # Plot execution times
        plt.figure(figsize=(10, 6))
        plt.plot(df["extract(seconds)"], label="Extract", marker="o")
        plt.plot(df["transform(seconds)"], label="Transform", marker="o")
        plt.plot(df["load(seconds)"], label="Load", marker="o")
        plt.plot(df["analyze(seconds)"], label="Analyze", marker="o")

        plt.title("ETL Task Durations per DAG Run")
        plt.xlabel("Run index")
        plt.ylabel("Duration (seconds)")
        plt.legend()
        plt.tight_layout()

        # Save the plot
        output_file = vis_dir / "etl_metrics_overview.png"
        plt.savefig(output_file)
        plt.close()

        log.info(f"Metrics visualization saved at {output_file}")

    except Exception as e:
        log.error(f"Error during metrics visualization: {e}")
        raise
