# download_data.py
import random

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from utils.logger import get_logger

logger = get_logger(__name__.split(".")[-1])


def get_random_wiki_gz_link(month_url: str) -> str:
    """Fetch a random gzip file link from the Wikimedia hourly dumps page."""
    html = requests.get(month_url).text
    soup = BeautifulSoup(html, "lxml")

    links = [
        href
        for a in soup.find_all("a", href=True)
        if (href := a.get("href")) and isinstance(href, str) and href.endswith(".gz")
    ]

    if not links:
        raise ValueError("No gzip links found on the page.")

    chosen = random.choice(links)
    logger.info(f"Selected gzip file: {chosen}")
    return month_url + chosen


def download_file(link: str, destination: str, chunk_size: int = 1024) -> str:
    """Download a file with a progress bar."""
    with requests.get(link, stream=True) as req:
        req.raise_for_status()
        total = int(req.headers.get("content-length", 0))
        with (
            open(destination, "wb") as f,
            tqdm(
                total=total, unit="B", unit_scale=True, desc=destination, ascii=True
            ) as bar,
        ):
            for chunk in req.iter_content(chunk_size):
                f.write(chunk)
                bar.update(len(chunk))
    logger.success(f"File downloaded successfully: {destination}")
    return destination
