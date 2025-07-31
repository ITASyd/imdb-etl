import pandas as pd
import matplotlib.pyplot as plt
from airflow.utils.log.logging_mixin import LoggingMixin
from dags.config import (RAW,
                         VISUALIZATIONS,
                         SEPARATOR,
                         MISSING_VALUE)

def extract_visualize():
    """
    Visualizes the number of rows in each extracted IMDb dataset.
    Saves a bar chart in data/visualizations/extract_summary.png.
    """
    log = LoggingMixin().log

    raw_dir = RAW
    vis_dir = VISUALIZATIONS
    vis_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Row count for each .tsv in data/raw
        row_counts = {}
        for file in raw_dir.glob("*.tsv"):
            df = pd.read_csv(file, sep=SEPARATOR, na_values=MISSING_VALUE)
            row_counts[file.name] = len(df)
            log.info(f"{file.name}: {len(df)} rows")

        # Create DataFrame for better visualization
        summary_df = pd.DataFrame(list(row_counts.items()), columns=["file", "rows"])
        summary_df.sort_values(by="rows", ascending=False, inplace=True)

        # Graph
        plt.figure(figsize=(10, 6))
        plt.bar(summary_df["file"], summary_df["rows"], color="skyblue")
        plt.title("Rows per Extracted IMDb Dataset")
        plt.xlabel("File")
        plt.ylabel("Number of Rows")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()

        # Export graph
        output_file = vis_dir / "extract_summary.png"
        plt.savefig(output_file)
        plt.close()
        log.info(f"Visualization saved to {output_file}")

    except Exception as e:
        log.error(f"Error during extract visualization: {e}")
        raise
