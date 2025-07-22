import urllib.request
import gzip
import shutil
from pathlib import Path
import os
from dags.config import RAW, FILES

def extract():
    """
    Downloads IMDb datasets, decompresses them, and saves them into data/raw/.
    """

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
            print(f"{zipped_path} not found, skipping deletion.")