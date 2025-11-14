# download_data.py

import gzip
import logging
import os
import random
import shutil
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from core_sentiment.include.app_config.settings import config
from tqdm import tqdm

logger = logging.getLogger(__name__)


class DownloadError(Exception):
    """Custom exception for download errors"""

    pass


def get_random_wiki_gz_link(url: str) -> str:
    """
    Function:
        - Fetch a random gzip file link from the Wikimedia hourly dumps page.

    Argument
        - url: URL to pick a random url link from

    Return:
        - url: URL of a pageview link
    """

    logger.info(f"Fetching gzip file links from: {url}")
    html = requests.get(url).text
    soup = BeautifulSoup(html, "lxml")

    links = [
        href
        for a in soup.find_all("a", href=True)
        if (href := a.get("href")) and isinstance(href, str) and href.endswith(".gz")
    ]

    if not links:
        raise DownloadError("No gzip links found on the page.")

    chosen = random.choice(links)
    full_url = url + chosen
    logger.info(f"Selected gzip file: {full_url}")
    return full_url


def download_file(url: str, destination: Path, chunk_size: int = 8192) -> Path:
    """
    Function:
        - Downloads a file from the specified URL and save it into RAW_PAGE_VIEWS_DIR.
        - Automatically switches between tqdm (for local runs) and shutil (for Airflow/headless runs).

    Arguments:
        - url: URL to download from
        - destination: Local path to save the file
        - chunk_size: Size of chunks to download

    Return:
        - Path to downloaded file

    Raise:
        DownloadError: If download fails or file is corrupted
    """

    try:
        logger.info(f"Downloading from: {url}")

        # Start the request
        with requests.get(url, stream=True, timeout=30) as response:
            response.raise_for_status()
            total_size = int(response.headers.get("content-length", 0))

            # Use tqdm for progress display in interactive mode
            use_progress_bar = os.isatty(1)

            if use_progress_bar:
                logger.info("Using tqdm progress bar (interactive mode activated)")
                with (
                    open(destination, "wb") as f,
                    tqdm(
                        total=total_size,
                        unit="B",
                        unit_scale=True,
                        unit_divisor=1024,
                        desc=destination.name,
                        ascii=True,
                    ) as bar,
                ):
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            size = f.write(chunk)
                            bar.update(size)

            else:
                with open(destination, "wb") as f:
                    shutil.copyfileobj(response.raw, f)

        logger.info(f"File downloaded successfully: {destination}")
        return destination

    except requests.RequestException as e:
        if destination.exists():
            destination.unlink(missing_ok=True)  # Cleanup partial download
        logger.error(f"Network-related error: {e}")
        raise DownloadError(f"Failed to download {url}: {e}")

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise DownloadError(str(e))


def validate_gz_file(file_path: Path) -> bool:
    """
    Function:
        - Validate that a file is a valid gzip file.

    Argument:
        - file_path: Path to the file to validate.

    Return:
        - True if gzip file is valid.
        - False if validation fails.
    """

    try:
        with gzip.open(file_path, "rb") as f:
            f.read(1)  # Try reading the first byte
        logger.info(f"File validation passed: {file_path}")
        return True
    except Exception as e:
        logger.error(f"Invalid gzip file {file_path}: {e}")
        return False


def download_random_wiki_file() -> str:
    """
    Function:
        Wrapper function that
            - Picks a random .gz link from the given Wikimedia URL
            - Downloads it into RAW_PAGEVIEWS_DIR
            - Validates the gzip file

    Argument:
        - None

    Return:
        - Path to the validated downloaded gzip file if successful.

    Raise:
        - DownloadError if download or validation fails.
    """

    # Base Wikipedia monthly pageview URL for October (you can change month here).
    MONTH_URL = "https://dumps.wikimedia.org/other/pageviews/2025/2025-10/"

    try:
        logger.info("Starting download process...")
        # Get a random link
        file_url = get_random_wiki_gz_link(MONTH_URL)

        # Prepare the destination
        raw_dir = Path(config.RAW_PAGEVIEWS_DIR)
        raw_dir.mkdir(parents=True, exist_ok=True)
        file_path = raw_dir / Path(file_url).name

        # Download the file
        downloaded_path = download_file(file_url, file_path)

        # Validate the gzip file
        if not validate_gz_file(downloaded_path):
            raise DownloadError(
                f"Downloaded file is not a valid gzip: {downloaded_path}"
            )

        logger.info(f"File successfully downloaded and validated: {downloaded_path}")
        return str(downloaded_path)

    except DownloadError as e:
        logger.error(f"Download failed: {e}")
        raise

    except Exception as e:
        logger.exception(f"Unexpected error during download: {e}")
        raise
