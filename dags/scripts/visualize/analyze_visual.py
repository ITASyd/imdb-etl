import pandas as pd
import matplotlib.pyplot as plt
from airflow.utils.log.logging_mixin import LoggingMixin
from dags.config import (VISUALIZATIONS)

def analyze_visualize(**context):
    """
    Generates and saves a horizontal bar chart visualizing the top 3 movies by average rating.
    This function retrieves analysis results from an Airflow XCom, processes the data to extract movie titles and their vote counts,
    and creates a horizontal bar chart using matplotlib. The resulting visualization is saved as a PNG file in the specified
    visualizations directory.
    """
    log = LoggingMixin().log
    vis_dir = VISUALIZATIONS

    try:
        data = context["ti"].xcom_pull(task_ids="analyze")
        best_json = data["result"]
        best = pd.read_json(best_json)
        best_title_votes = best[["title", "votes"]]

        # Graph
        plt.figure(figsize=(10, 6))
        plt.barh(best_title_votes["title"], best_title_votes["votes"], color="yellow")
        plt.title("Most voted movies")
        plt.ylabel("Movie")
        plt.xlabel("Number of votes")
        plt.tight_layout()

        # Export graph
        output_file = vis_dir / "analyze_summary.png"
        plt.savefig(output_file)
        plt.close()
        log.info(f"Visualization saved to {output_file}")
    
    except Exception as e:
        log.error(f"Error during analyze visualization: {e}")
        raise