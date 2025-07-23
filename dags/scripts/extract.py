import urllib.request
import gzip
import shutil
import os
from dags.config import RAW, FILES
from airflow.utils.log.logging_mixin import LoggingMixin

def extract():
    """
    Downloads IMDb datasets, decompresses them, and saves them into data/raw/.
    """
    log = LoggingMixin().log

    try:
        RAW.mkdir(parents=True, exist_ok=True)

        for filename_gz, url in FILES.items():
            zipped_path = RAW / filename_gz
            unzipped_path = RAW / filename_gz[:-3]
            urllib.request.urlretrieve(url, zipped_path)
            with gzip.open(zipped_path, 'rb') as zipped:
                with open(unzipped_path, 'wb') as unzipped:
                    shutil.copyfileobj(zipped, unzipped)
            try:
                os.remove(zipped_path)
            except FileNotFoundError:
                log.error(f"{zipped_path} not found, skipping deletion.")
    except Exception as e:
        log.error(f"Error during extraction: {e}")
        raise