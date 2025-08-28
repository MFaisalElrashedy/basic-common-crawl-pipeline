from abc import ABC, abstractmethod
import csv
import gzip
from typing import Generator, List, Optional
import requests
import os


CRAWL_PATH = "cc-index/collections/{}/indexes"
BASE_URL = "https://data.commoncrawl.org"


def crawl_path(version: str = "CC-MAIN-2024-30") -> str:
    """Return the index path for a given crawl version (default: CC-MAIN-2024-30)."""
    return CRAWL_PATH.format(version)


class Downloader(ABC):
    @abstractmethod
    def download_and_unzip(self, url: str, start: int, length: int) -> bytes:
        pass


class CCDownloader(Downloader):
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    def download_and_unzip(self, url: str, start: int, length: int) -> bytes:
        headers = {"Range": f"bytes={start}-{start+length-1}"}
        response = requests.get(f"{self.base_url}/{url}", headers=headers, verify=False)
        response.raise_for_status()
        buffer = response.content
        return gzip.decompress(buffer)

class CCIndexDownloader:
    """Downloader for cluster.idx file of a given crawl version."""

    def __init__(self, crawl_version: str, output_folder: str) -> None:
        self.crawl_version = crawl_version
        self.output_folder = os.path.abspath(output_folder)
        os.makedirs(self.output_folder, exist_ok=True)

    def download(self) -> str:
        """Always download cluster.idx for the configured crawl version (overwrite existing)."""
        url = f"{BASE_URL}/{crawl_path(self.crawl_version)}/cluster.idx"
        local_file = os.path.join(self.output_folder, f"cluster-{self.crawl_version}.idx")

        print(f"[CCIndexDownloader] Downloading cluster.idx from {url}")
        response = requests.get(url, stream=True, verify=False)
        response.raise_for_status()
        with open(local_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"[CCIndexDownloader] Saved cluster.idx to {local_file}")
        return local_file


class IndexReader(ABC):
    @abstractmethod
    def __iter__(self):
        pass


class CSVIndexReader(IndexReader):
    def __init__(self, filename: str) -> None:
        self.file = open(filename, "r")
        self.reader = csv.reader(self.file, delimiter="\t")

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.reader)

    def __del__(self) -> None:
        self.file.close()


def test_can_read_index(tmp_path):
    filename = tmp_path / "test.csv"
    index = "0,100,22,165)/ 20240722120756	cdx-00000.gz	0	188224	1\n\
101,141,199,66)/robots.txt 20240714155331	cdx-00000.gz	188224	178351	2\n\
104,223,1,100)/ 20240714230020	cdx-00000.gz	366575	178055	3"
    filename.write_text(index)
    reader = CSVIndexReader(filename)
    assert list(reader) == [
        ["0,100,22,165)/ 20240722120756", "cdx-00000.gz", "0", "188224", "1"],
        [
            "101,141,199,66)/robots.txt 20240714155331",
            "cdx-00000.gz",
            "188224",
            "178351",
            "2",
        ],
        ["104,223,1,100)/ 20240714230020", "cdx-00000.gz", "366575", "178055", "3"],
    ]
