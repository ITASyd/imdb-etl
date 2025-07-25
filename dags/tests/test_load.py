import pytest
from unittest import mock
import sys
from types import SimpleNamespace

# LoggingMixin mock for testing in Windows environment
tmp_log = SimpleNamespace(log=mock.Mock())
sys.modules["airflow.utils.log.logging_mixin"] = SimpleNamespace(LoggingMixin=lambda: tmp_log)

@mock.patch("dags.scripts.load.create_engine")
@mock.patch("dags.scripts.load.pd.read_csv")
@mock.patch("dags.scripts.load.pd.read_sql")
def test_load(mock_read_sql, mock_read_csv, mock_create_engine):
    # Mock setup
    mock_engine = mock.Mock()
    mock_create_engine.return_value = mock_engine

    mock_genre_df = mock.Mock()
    mock_best_df = mock.Mock()
    mock_read_csv.side_effect = [mock_genre_df, mock_best_df]

    mock_read_sql.side_effect = [mock.Mock(), mock.Mock()]

    # Patch to_sql on each DataFrame e ignora la validazione Pandera
    with mock.patch.object(mock_genre_df, 'to_sql') as mock_to_sql_genre, \
         mock.patch.object(mock_best_df, 'to_sql') as mock_to_sql_best, \
         mock.patch("dags.scripts.load.BestSchema.validate", return_value=None), \
         mock.patch("dags.scripts.load.GenreSchema.validate", return_value=None):
        from dags.scripts import load
        load.load()

        mock_create_engine.assert_called_once()
        assert mock_read_csv.call_count == 2
        assert mock_to_sql_genre.call_count == 1
        assert mock_to_sql_best.call_count == 1
        assert mock_read_sql.call_count == 2
