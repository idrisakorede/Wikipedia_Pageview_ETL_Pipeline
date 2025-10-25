# extract_data.py
"""Data extraction utilities for Wikipedia pageview data"""

import gzip
from pathlib import Path
from typing import Tuple
import pandas as pd

from configs import COMPANY_NAMES, COMPANY_ALIASES, CHUNK_SIZE
from utils.logger import get_logger

logger = get_logger(__name__.split(".")[-1])


class ExtractionError(Exception):
    """Custom exception for extraction errors."""

    pass


def extract_company_data(
    zipped_file: str, output_file: str = None, chunk_size: int = None
) -> Tuple[pd.DataFrame, str]:
    """
    Extract and normalize company-related Wikipedia pageviews from a gzip file.

    Args:
        zipped_file: Path to the gzip file containing pageview data
        output_file: Path to save the extracted CSV file
        chunk_size: Number of rows to process per chunk

    Returns:
        Tuple of (DataFrame with extracted data, output file path)

    Raises:
        ExtractionError: If extraction fails
    """

    if chunk_size is None:
        chunk_size = CHUNK_SIZE

    if output_file is None:
        output_file = Path(zipped_file).parent / "company_views.csv"

    logger.info(f"Extracting company data from: {zipped_file}")
    logger.info(f"Processing in chunks of {chunk_size:,} rows")

    # Validate input file exists
    if not Path(zipped_file).exists():
        raise ExtractionError(f"Input file not found: {zipped_file}")

    # Validate it is a gzip file
    try:
        with gzip.open(zipped_file, "rt") as f:
            f.readline()  # Test if we can read
    except Exception as e:
        raise ExtractionError(f"Invalid gzip file: {e}")

    columns = ["domain_code", "page_title", "count_views", "total_response_size"]
    company_views = []
    total_rows_processed = 0
    total_matches = 0

    try:
        for chunk_num, chunk in enumerate(
            pd.read_csv(
                zipped_file,
                sep=" ",
                names=columns,
                compression="gzip",
                usecols=["domain_code", "page_title", "count_views"],
                chunksize=chunk_size,
                on_bad_lines="skip",
                engine="python",
            ),
            start=1,
        ):
            total_rows_processed += len(chunk)

            # Filter for company pages
            filtered = chunk.loc[chunk["page_title"].isin(COMPANY_NAMES)].copy()
            if filtered.empty:
                continue

            # Normalize page titles and map to company names
            filtered["page_title"] = filtered["page_title"].str.lower()
            filtered["company_name"] = filtered["page_title"].map(COMPANY_ALIASES)
            filtered = filtered.dropna(subset=["company_name"])
            # company_views.append(filtered)

            # Clean and validate view counts
            filtered["count_views"] = (
                pd.to_numeric(filtered["count_views"], errors="coerce").fillna(0).astype(int)
            )

            # Remove zero or negative counts
            filtered = filtered[filtered["count_views"] > 0]

            if not filtered.empty:
                total_matches += len(filtered)
                company_views.append(filtered)
                logger.info(
                    f"Chunk {chunk_num}: Found {len(filtered)} matching records "
                    f"(Total: {total_matches})"
                )

        logger.info(f"Processed {total_rows_processed:,} total rows")

        if not company_views:
            logger.warning("No matching company data found in file.")
            # Create empty Dataframe with correct schema
            result = pd.DataFrame(columns=["company_name", "count_views"])
            result.to_csv(output_file, index=False)
            return result, str(output_file)

        # Combine all chunks
        result = pd.concat(company_views, ignore_index=True)

        # Aggregated views by company (Sum all occurrences)
        aggregated = (
            result.groupby("company_name")["count_views"]
            .sum()
            .reset_index()
            .sort_values("count_views", ascending=False)
        )

        # Save to CSV with only required columns
        aggregated.to_csv(output_file, index=False)

        logger.success(
            f"Extraction completed: {len(aggregated)} companies, "
            f"{aggregated['count_views'].sum():,} total views"
        )
        logger.info(f"Data saved to: {output_file}")

        # Log summary
        for _, row in aggregated.iterrows():
            logger.info(f"  {row['company_name']}: {row['count_views']:,} views")

        return aggregated, str(output_file)

    except pd.errors.EmptyDataError:
        raise ExtractionError("Input file is empty")

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise ExtractionError(f"Failed to extract data: {e}")


def validate_extraction_output(csv_path: str) -> bool:
    """
    Validate the extracted CSV file

    Args:
        csv_path: Path to the CSV file

    Returns:
        True if valid, False otherwise
    """

    try:
        df = pd.read_csv(csv_path)

        # Check required columns
        required_cols = ["company_name", "count_views"]
        if not all(col in df.columns for col in required_cols):
            logger.error(f"Missing required columns. Found: {df.columns.to_list()}")
            return False

        # Check for data
        if df.empty:
            logger.warning("Extracted file is empty")
            return False

        # Check data types
        if not pd.api.types.is_integer_dtype(df["count_views"]):
            logger.error("Found negative view counts")
            return False

        logger.info(f"Validation passed: {len(df)} companies in output")
        return True

    except Exception as e:
        logger.error(f"Validation failed: {e}")
        return False
