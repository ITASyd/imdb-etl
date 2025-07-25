import pytest
from unittest import mock

# LoggingMixin mock for testing in Windows environment
import sys
from types import SimpleNamespace
mock_logging_mixin = SimpleNamespace(log=mock.Mock())
sys.modules["airflow.utils.log.logging_mixin"] = SimpleNamespace(LoggingMixin=lambda: mock_logging_mixin)

from dags.scripts import extract

@mock.patch("dags.scripts.extract.urllib.request.urlretrieve")
@mock.patch("dags.scripts.extract.gzip.open")
@mock.patch("dags.scripts.extract.shutil.copyfileobj")
@mock.patch("dags.scripts.extract.os.remove")
@mock.patch("dags.scripts.extract.RAW")
@mock.patch("dags.scripts.extract.FILES", {"sample.tsv.gz": "http://example.com/sample.tsv.gz"})
def test_extract_success(mock_raw, mock_remove, mock_copyfile, mock_gzip_open, mock_urlretrieve):
    # Preparazione mock
    mock_raw.__truediv__.side_effect = lambda filename: f"/mocked/path/{filename}"
    mock_raw.mkdir.return_value = None

    mock_file_in = mock.Mock()
    mock_file_out = mock.Mock()
    mock_gzip_open.return_value.__enter__.return_value = mock_file_in
    mock_open_builtin = mock.mock_open()
    
    with mock.patch("builtins.open", mock_open_builtin):
        extract.extract()

    # Verifiche
    mock_raw.mkdir.assert_called_once_with(parents=True, exist_ok=True)
    mock_urlretrieve.assert_called_once_with("http://example.com/sample.tsv.gz", "/mocked/path/sample.tsv.gz")
    mock_gzip_open.assert_called_once_with("/mocked/path/sample.tsv.gz", 'rb')
    mock_open_builtin.assert_called_once_with("/mocked/path/sample.tsv", 'wb')
    mock_copyfile.assert_called_once_with(mock_file_in, mock_open_builtin())
    mock_remove.assert_called_once_with("/mocked/path/sample.tsv.gz")
