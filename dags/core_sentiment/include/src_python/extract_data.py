# extract_data.py
import gzip
import logging

import pandas as pd

logger = logging.getLogger(__name__)


class ExtractionError(Exception):
    """Custom exception for extraction errors."""

    pass


def extract_company_data(
    zipped_file: str, output_file: str = "all_pageviews.csv"
) -> str:
    """
    Function:
        - Extract and store Wikipedia pageview data from a gzip file without filtering.
        - It reads the gzip-compressed file in chunks, selects specific columns, and saves all combined results in a CSV file.

    Arguments:
        - zipped_file: Path to the gzip-compressed Wikipedia pageview file.
        - output_file: Name of the output CSV file to save extracted data. Defaults to "all_pageviews.csv".

    Return:
        - Path to the saved CSV file upon successful extraction.
        - Returns an empty string if extraction fails or no data found.

    Raise:
        - ExtractionError if extraction fails
    """
    logger.info(f"Reading and filtering gzip file in chunks: {zipped_file}")

    # Validate the gzip file
    try:
        with gzip.open(zipped_file, "rt") as f:
            f.readline()  # Test if we can read
    except Exception as e:
        raise ExtractionError(f"Invalid gzip file: {e}")

    chunk_size = 500_000
    columns = ["domain_code", "page_title", "count_views", "total_response_size"]

    all_pageviews = []

    try:
        for chunk in pd.read_csv(
            zipped_file,
            sep=" ",
            names=columns,
            compression="gzip",
            usecols=["domain_code", "page_title", "count_views"],
            chunksize=chunk_size,
            on_bad_lines="skip",
        ):
            # Collect each chunk
            all_pageviews.append(chunk)

        if all_pageviews:
            # Combine all chunks into a DataFrame
            result = pd.concat(all_pageviews, ignore_index=True)

            # Save combined data into a CSV
            result.to_csv(output_file, index=False)

            logger.info(f"Extraction completed successfully. Saved to {output_file}")
            return output_file
        else:
            logger.warning("No data found in the gzip file.")
            return ""

    except Exception as e:
        logger.error(f"Error during extraction: {e}")
        raise ExtractionError(f"Extraction failed: {e}")


def validate_extraction_output(csv_path: str) -> bool:
    """
    Function:
        - Validate that the extracted CSV file is valid and properly formatted.

    Argument:
        - csv_path: Path to the CSV file to be validated.

    Return:
        - True if the CSV file is valid.
        - False if validation fails.
    """

    try:
        df = pd.read_csv(csv_path)

        # Check if the required columns exist
        required_columns = ["domain_code", "page_title", "count_views"]
        if not all(column in df.columns for column in required_columns):
            logger.error(f"Missing required columns. Found: {df.columns.to_list()}")
            return False

        # Check that file is not empty
        if df.empty:
            logger.warning("Extracted file is empty")
            return False

        # Check that count_views column contains integers
        if not pd.api.types.is_integer_dtype(df["count_views"]):
            logger.error("Invalid data type in 'count_views' column.")
            return False

        logger.info("Validation passed")
        return True

    except Exception as e:
        logger.error(f"Validation failed: {e}")
        return False


def extract_data(zipped_file: str) -> str:
    """
    Function:
        - Wrapper function that coordinates the extraction and validation steps.
        - It extracts data from gzip file and validates the resulting CSV file.

    Argument:
        - zipped_file: Path to the gzip-compressed Wikipedia pageview file.

    Return:
        - Path to the validated CSV file if successful.

    Raise:
        - ExtractionError if extraction or validation fails.

    """

    try:
        logger.info("Starting extraction process...")

        # Extract data
        csv_file = extract_company_data(zipped_file)

        # Validate output
        if not csv_file or not validate_extraction_output(csv_file):
            raise ExtractionError("Extraction validation failed.")

        logger.info("Extraction completed and validated successfully.")
        return csv_file

    except ExtractionError as e:
        logger.error(f"Extraction failed: {e}")
        raise

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise
