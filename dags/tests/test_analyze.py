import pytest
from unittest import mock
import pandas as pd
import sys
from types import SimpleNamespace

# LoggingMixin mock for testing in Windows environment
tmp_log = SimpleNamespace(log=mock.Mock())
sys.modules["airflow.utils.log.logging_mixin"] = SimpleNamespace(LoggingMixin=lambda: tmp_log)

@mock.patch("dags.scripts.analyze.create_engine")
@mock.patch("dags.scripts.analyze.pd.read_sql_table")
@mock.patch("dags.scripts.analyze.pd.read_sql_query")
def test_analyze(mock_read_sql_query, mock_read_sql_table, mock_create_engine):
    mock_engine = mock.Mock()
    mock_create_engine.return_value = mock_engine

    mock_read_sql_table.return_value = mock.Mock()
    mock_read_sql_query.return_value = mock.Mock()

    from dags.scripts import analyze
    analyze.analyze()

    mock_create_engine.assert_called_once()
    mock_read_sql_table.assert_called_once_with(table_name="best_films_per_genre", con=mock_engine)
    mock_read_sql_query.assert_called_once()