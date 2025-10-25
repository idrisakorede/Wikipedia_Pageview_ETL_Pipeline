# download_data.py
"""Download utilities for Wikipedia pageview data"""

import random
from pathlib import Path
import gzip
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


from utils.logger import get_logger


logger = get_logger(__name__)


class DownloadError(Exception):
    """Custom exception for download errors"""

    pass


def get_random_wiki_gz_link(month_url: str) -> str:
    """
    Fetch a random gzip file link from the Wikimedia hourly dumps page.

    Args:
        month_url: URL to the Wikimedia dumps page for a specific month

    Returns:
        Full URL to a random gzip file

    Raises:
        DownloadError: If no gzip files found or connection fails
    """

    try:
        logger.info(f"Fetching available files from: {month_url}")
        response = requests.get(month_url, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "lxml")

        links = [
            href
            for a in soup.find_all("a", href=True)
            if (href := a.get("href")) and isinstance(href, str) and href.endswith(".gz")
        ]

        if not links:
            raise DownloadError("No gzip links found on the page.")

        pick = random.choice(links)
        full_url = month_url + pick if not pick.startswith("http") else pick

        logger.success(f"Selected gzip file: {pick}")
        return full_url

    except requests.RequestException as e:
        logger.error(f"Failed to fetch file list: {e}")
        raise DownloadError(f"Connection error: {e}")


def download_file(
    url: str, destination: str, chunk_size: int = 8192, verify_size: bool = True
) -> str:
    """
    Download a file with a progress bar and validation.

    Args:
        url: URL to download from
        destination: Local path to save the file
        chunk_size: Size of chunks to download
        verify_size: Whether to verify downloaded file size

    Returns:
        Path to downloaded file

    Raises:
        DownloadError: If download fails or file is corrupted
    """

    dest_path = Path(destination)
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    # Check if file already exists and is valid
    if dest_path.exists() and verify_size:
        logger.info(f"File already exist: {destination}")
        return str(dest_path)

    try:
        logger.info(f"Downloading from: {url}")
        with requests.get(url, stream=True, timeout=30) as response:
            response.raise_for_status()
            total_size = int(response.headers.get("content-length", 0))

            if total_size == 0:
                logger.warning("Content-length header missing, size verification disabled")

            with (
                open(dest_path, "wb") as f,
                tqdm(
                    total=total_size,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=dest_path.name,
                    ascii=True,
                ) as bar,
            ):
                download_size = 0
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        size = f.write(chunk)
                        download_size += size
                        bar.update(size)

            # Verify download
            if verify_size and total_size > 0:
                actual_size = dest_path.stat().st_size
                if actual_size != total_size:
                    dest_path.unlink()  # Delete corrupted file
                    raise DownloadError(f"Size mismatch: expected {total_size}, got {actual_size}")
            logger.success(f"File downloaded successfully: {destination} ({download_size:,} bytes)")
            return str(dest_path)

    except requests.RequestException as e:
        if dest_path.exists():
            dest_path.unlink()  # Cleanup partial download
        logger.error(f"Download failed: {e}")
        raise DownloadError(f"Failed to download {url}: {e}")

    except Exception as e:
        if dest_path.exists():
            dest_path.unlink()
        logger.error(f"Unexpected error during download: {e}")
        raise


def validate_gz_file(file_path: str) -> bool:
    """
    Validate that a file is a valid gzip file.

    Args:
        file_path: Path to the file to validate

    Returns:
        True if valid gzip file, False otherwise
    """

    try:
        with gzip.open(file_path, "rb") as f:
            f.read(1)  # Try reading the first byte
        logger.info(f"File validation passed: {file_path}")
        return True
    except Exception as e:
        logger.error(f"Invalid gzip file {file_path}: {e}")
        return False
