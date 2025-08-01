import pytest
from unittest import mock
import pandas as pd
import sys
from types import SimpleNamespace

# LoggingMixin mock for testing in Windows environment
tmp_log = SimpleNamespace(log=mock.Mock())
sys.modules["airflow.utils.log.logging_mixin"] = SimpleNamespace(LoggingMixin=lambda: tmp_log)

# Input data mock
BASICS_DF = pd.DataFrame({
    "tconst": ["tt1", "tt2"],
    "titleType": ["movie", "movie"],
    "primaryTitle": ["Film1", "Film2"],
    "startYear": [2000, 2001],
    "genres": ["Action,Comedy", "Comedy"]
})
RATINGS_DF = pd.DataFrame({
    "tconst": ["tt1", "tt2"],
    "averageRating": [8.0, 7.5],
    "numVotes": [15000, 8000]
})

@mock.patch("dags.scripts.transform.pd.read_csv")
@mock.patch("dags.scripts.transform.PROCESSED")
@mock.patch("dags.scripts.transform.os.path.getsize")
def test_transform(mock_getsize, mock_processed, mock_read_csv):
    import dags.scripts.transform as tr
    tr.GENRE_CSV = "genre_test.csv"
    tr.BEST_CSV = "best_test.csv"
    tr.MIN_VOTES = 10000

    # Mock setup
    mock_read_csv.side_effect = [BASICS_DF, RATINGS_DF]
    mock_processed.mkdir.return_value = None
    mock_getsize.return_value = 0

    # to_csv mock
    with mock.patch("pandas.DataFrame.to_csv") as mock_to_csv:
        tr.transform()
        # Check if data is written
        assert mock_to_csv.call_count == 2
        called_files = [call.args[0] for call in mock_to_csv.call_args_list]
        assert "genre_test.csv" in called_files
        assert "best_test.csv" in called_files
