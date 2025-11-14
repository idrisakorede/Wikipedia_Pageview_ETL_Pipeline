import logging
from pathlib import Path

import pandas as pd
from core_sentiment.include.app_config.settings import config

logger = logging.getLogger(__name__)


class PrefilterError(Exception):
    """Custom exception for prefilter errors"""

    pass


def prefilter_data(csv_file: str) -> str:
    """
    Function:
        - Pre-filter the pageview data to reduce volume before LLM processing.
        - Keeps all domain codes but filters for pages related to the five major tech companies.

    Argument:
        - csv_file: Path to the CSV file containing pageview data

    Return:
        - Path to the pre-filtered CSV file

    Raise:
        - PrefilterError: If prefiltering fails
    """

    try:
        logger.info(f"Reading CSV file: {csv_file}")
        df = pd.read_csv(csv_file)
        logger.info(f"Loaded {len(df)} total records")

        # Keep all columns as-is.
        df_filtered = df.copy()

        # Filter out special pages, templates, categories, etc.
        exclude_prefixes = [
            "Special:",
            "Template:",
            "Category:",
            "Wikipedia:",
            "Help:",
            "Portal:",
            "File:",
            "Talk:",
            "User:",
            "MediaWiki:",
            "Module:",
            "Draft:",
            "TimedText:",
        ]

        pattern = "|".join([f"^{prefix}" for prefix in exclude_prefixes])
        df_filtered = df_filtered[
            ~df_filtered["page_title"].str.contains(pattern, case=False, na=False)
        ]
        logger.info(f"After excluding special pages: {len(df_filtered)} records")

        # Filter for the five major tech companies
        # Include variations and common product/service names
        company_patterns = [
            # Amazon ecosystem
            r"\bAmazon\b",
            r"\bAWS\b",
            r"\bAlexa\b",
            r"\bKindle\b",
            r"\bPrime\b",
            r"\bFire_TV\b",
            r"\bEcho\b",
            # Apple ecosystem
            r"\bApple\b",
            r"\biPhone\b",
            r"\biPad\b",
            r"\bMac\b",
            r"\biOS\b",
            r"\bmacOS\b",
            r"\bAirPods\b",
            r"\biCloud\b",
            r"\bSafari\b",
            r"\bHomePod\b",
            r"\biTunes\b",
            # Meta/Facebook ecosystem
            r"\bMeta\b",
            r"\bFacebook\b",
            r"\bInstagram\b",
            r"\bWhatsApp\b",
            r"\bOculus\b",
            r"\bMessenger\b",
            # Google ecosystem
            r"\bGoogle\b",
            r"\bYouTube\b",
            r"\bAndroid\b",
            r"\bChrome\b",
            r"\bPixel\b",
            r"\bGmail\b",
            r"\bAlphabet\b",
            # Microsoft ecosystem
            r"\bMicrosoft\b",
            r"\bWindows\b",
            r"\bXbox\b",
            r"\bOffice\b",
            r"\bAzure\b",
            r"\bBing\b",
            r"\bLinkedIn\b",
            r"\bOneDrive\b",
            r"\bTeams\b",
            r"\bSurface\b",
        ]

        # Create regex pattern for company keywords (case-insensitive)
        company_pattern = "|".join(company_patterns)
        df_filtered = df_filtered[
            df_filtered["page_title"].str.contains(
                company_pattern, case=False, na=False, regex=True
            )
        ]
        logger.info(f"After company keyword filter: {len(df_filtered)} records")

        # # Keep only pages with significant views (e.g., > 50 views)
        # df_filtered = df_filtered[df_filtered["count_views"] > 50]
        # logger.info(f"After view count filter: {len(df_filtered)} records")

        # # Sort by view count for better quality results
        # df_filtered = df_filtered.sort_values("count_views", ascending=False)
        # logger.info(f"Final records for LLM processing: {len(df_filtered)} records")

        # Save pre-filtered data to processed directory
        processed_dir = Path(config.PROCESSED_PAGEVIEWS_DIR)
        processed_dir.mkdir(parents=True, exist_ok=True)

        # Extract original filename and create prefiltered version
        original_filename = Path(csv_file).stem  # Gets filename without extension
        prefiltered_file = processed_dir / f"{original_filename}_prefiltered.csv"

        df_filtered.to_csv(prefiltered_file, index=False)
        logger.info(f"Pre-filtered data saved to: {prefiltered_file}")

        return str(prefiltered_file)

    except Exception as e:
        logger.error(f"Error in pre-filtering: {e}")
        raise PrefilterError(f"Prefiltering failed: {e}")


# def bash_prefilter_data(csv_file):
#     """
#     Ultra-fast bash pre-filter using grep.
#     Stage 1: Keep only company-related pages.
#     Stage 2: Remove obvious noise.
#     """

#     # Save pre-filtered data to processed directory
#     processed_dir = Path(config.PROCESSED_PAGEVIEWS_DIR)
#     processed_dir.mkdir(parents=True, exist_ok=True)

#     # Extract original filename and create prefiltered version
#     original_filename = Path(csv_file).stem  # Gets filename without extension
#     prefiltered_file = processed_dir / f"{original_filename}_prefiltered.csv"

#     return f"""
#     set -e  # Exit on error

#     # Stage 1: Extract company-related pages
#     echo "Stage 1: Filtering for company-related pages..."
#     grep -E -i '(Amazon|AWS|Alexa|Kindle|Prime|Apple|iPhone|iPad|Mac|iOS|Meta|Facebook|Instagram|WhatsApp|Google|Android|Chrome|YouTube|Pixel|Microsoft|Windows|Xbox|Office|Azure)' "{csv_file}" > /tmp/stage1.csv

#     echo "Stage 1 complete. Rows: $(wc -l < /tmp/stage1.csv)"

#     # Stage 2: Remove noise patterns
#     echo "Stage 2: Removing noise patterns..."
#     grep -v -E -i '(_v\\._|_vs\\._|lawsuit|CEO|founder|director|executive|president)' /tmp/stage1.csv | \
#     grep -v -E -i '(TV_series|film|book|novel|documentary)' | \
#     grep -v -E -i '(building|campus|headquarters|warehouse|data_center)' | \
#     grep -v -E -i '(conference|summit|expo|keynote)' | \
#     grep -v -E -i '(History_of|Timeline_of|List_of|criticism|controversy|scandal)' | \
#     grep -v -E -i '(Season_|Episode_|advertisement)' | \
#     grep -v -E '\\([0-9]{{4}}\\)' | \
#     grep -v -E '(http|www\\.|\\.(com|org|net|io)(?!puter))' | \
#     grep -v -E -i '(disambiguation|Category:|Template:|Portal:)' \
#     > "{prefiltered_file}"

#     echo "Stage 2 complete. Final rows: $(wc -l < {prefiltered_file})"
#     echo "Reduction: $(wc -l < {csv_file}) → $(wc -l < {prefiltered_file})"

#     # Cleanup
#     rm /tmp/stage1.csv

#     echo "{prefiltered_file}"
#     """
