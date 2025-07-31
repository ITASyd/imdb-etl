import urllib.request
import gzip
import shutil
import os
import time
import pandas as pd
from datetime import datetime
from dags.config import RAW, FILES, SEPARATOR, MISSING_VALUE
from airflow.utils.log.logging_mixin import LoggingMixin
from urllib.error import URLError

def extract():
    """
    Downloads IMDb datasets, decompresses them, and saves them into data/raw/.
    """
    start = time.time()
    start_date = datetime.now()
    start_date = start_date.strftime("%Y-%m-%d")
    log = LoggingMixin().log

    RAW.mkdir(parents=True, exist_ok=True)

    for filename_gz, url in FILES.items():
        zipped_path = RAW / filename_gz
        unzipped_path = RAW / filename_gz[:-3]

        try:
            urllib.request.urlretrieve(url, zipped_path)
        except URLError as e:
            log.error(f"Error during download from {url}: {e}")
            continue

        try:
            with gzip.open(zipped_path, 'rb') as zipped:
                with open(unzipped_path, 'wb') as unzipped:
                    shutil.copyfileobj(zipped, unzipped)
            try:
                os.remove(zipped_path)
            except FileNotFoundError:
                log.error(f"{zipped_path} not found, skipping deletion.")
                
        except (OSError, gzip.BadGzipFile) as e:
            log.error(f"Error during {zipped_path} extraction: {e}")
            continue
    
    # Data for metrics
    duration = time.time() - start
    duration = f"{duration:.2f}"
    row_count = 0
    files_size = 0
    for file in RAW.glob("*.tsv"):
        df = pd.read_csv(file, sep=SEPARATOR, na_values=MISSING_VALUE)
        row_count += len(df)
        files_size += os.path.getsize(file)
    log.info(f"duration: {duration}, row count: {row_count}, file size: {files_size}")
    return {
        "duration": duration,
        "start_date": start_date,
        "row_count": row_count,
        "files_size": files_size
    }