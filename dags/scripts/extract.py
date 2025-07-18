import urllib.request
import gzip
import shutil
from pathlib import Path
import os

def extract():
    """
    Downloads IMDb datasets, decompresses them, and saves them into data/raw/.
    """
    files = {
        "title.basics.tsv.gz": "https://datasets.imdbws.com/title.basics.tsv.gz",
        "title.ratings.tsv.gz": "https://datasets.imdbws.com/title.ratings.tsv.gz",
        "name.basics.tsv.gz": "https://datasets.imdbws.com/name.basics.tsv.gz"
    }

    raw_dir = Path("/opt/airflow/data/raw") #DOCKER
    #raw_dir = Path("data/raw") #LOCAL
    raw_dir.mkdir(parents=True, exist_ok=True)

    for filename_gz, url in files.items():
        zipped_path = raw_dir / filename_gz
        unzipped_path = raw_dir/ filename_gz[:-3]
        urllib.request.urlretrieve(url, zipped_path)
        with gzip.open(zipped_path, 'rb') as zipped:
            with open(unzipped_path, 'wb') as unzipped:
                shutil.copyfileobj(zipped, unzipped)
        try:
            os.remove(zipped_path)
        except FileNotFoundError:
            print(f"{zipped_path} not found, skipping deletion.")