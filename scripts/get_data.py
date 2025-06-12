import gzip
import os
from typing import List

import requests


def get_crawl_file_list(version: str = 'CC-MAIN-2024-10', limit: int = 5) -> List[str]:
    """Fetch list of WARC file paths from the Common Crawl index for the given version."""
    index_url = f'https://data.commoncrawl.org/crawl-data/{version}/warc.paths.gz'
    local_gz_path = f'{version}-warc.paths.gz'

    # Download warc.paths.gz file
    if not os.path.exists(local_gz_path):
        print(f'Downloading WARC paths index for {version}...')
        response = requests.get(index_url)
        with open(local_gz_path, 'wb') as f:
            f.write(response.content)
    else:
        print(f'Using cached index: {local_gz_path}')

    # Extract the list of WARC paths (first `limit` lines)
    warc_paths = []
    with gzip.open(local_gz_path, 'rt') as f:
        for i, line in enumerate(f):
            warc_paths.append(line.strip())
            if i + 1 >= limit:
                break

    return warc_paths


def download_warc_files(warc_paths: List[str], output_dir: str = './data') -> None:
    """Downloads WARC files from the Common Crawl data domain into the specified directory."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    base_url = 'https://data.commoncrawl.org/'
    for path in warc_paths:
        url = base_url + path
        filename = path.split('/')[-1]
        output_path = os.path.join(output_dir, filename)
        if os.path.exists(output_path):
            print(f'Skipping {filename} (already downloaded)')
            continue
        print(f'Downloading {filename} ...')
        os.system(f'wget -nc -O {output_path} {url}')


if __name__ == '__main__':
    version = 'CC-MAIN-2024-10'  # Parameter: Common Crawl version
    number_of_files = 1  # Parameter: Number of files to download
    output_folder = './data'  # Downloads will be saved here

    warc_paths = get_crawl_file_list(version=version, limit=number_of_files)
    download_warc_files(warc_paths, output_dir=output_folder)
