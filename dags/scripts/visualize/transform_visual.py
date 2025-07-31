import pandas as pd
import matplotlib.pyplot as plt
from airflow.utils.log.logging_mixin import LoggingMixin
from dags.config import (VISUALIZATIONS,
                         GENRE_CSV)

def transform_visualize():
    """
    Visualize the top 10 genres for film count in a graph.
    """

    log = LoggingMixin().log
    vis_dir = VISUALIZATIONS

    try:
        # Create the top10
        genres = pd.read_csv(GENRE_CSV)
        genres = genres.sort_values(by="film_count", ascending=False)
        genres_top10 = genres.head(10)

        # Graph
        plt.figure(figsize=(10, 6))
        plt.barh(genres_top10["genre"], genres_top10["film_count"], color="green")
        plt.title("Film count per genre - Top 10")
        plt.ylabel("Genres")
        plt.xlabel("Film count")
        plt.tight_layout()

        # Export graph
        output_file = vis_dir / "transform_summary.png"
        plt.savefig(output_file)
        plt.close()
        log.info(f"Visualization saved to {output_file}")
    except Exception as e:
        log.error(f"Error during transform visualization: {e}")
        raise

