import logging
from pathlib import Path

import pandas as pd
from core_sentiment.include.configuration.settings import config

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
        company_keywords = [
            # Amazon
            "Amazon",
            "AWS",
            "Alexa",
            "Kindle",
            "Prime_Video",
            "Amazon_Web_Services",
            "Fire_TV",
            "Echo_(device)",
            "Amazon_Prime",
            # Apple
            "Apple",
            "iPhone",
            "iPad",
            "Mac",
            "MacBook",
            "iOS",
            "macOS",
            "Apple_Watch",
            "AirPods",
            "iTunes",
            "Safari_(web_browser)",
            "iCloud",
            "App_Store",
            "Apple_TV",
            "HomePod",
            # Facebook/Meta
            "Facebook",
            "Meta_Platforms",
            "Instagram",
            "WhatsApp",
            "Oculus",
            "Meta_Quest",
            "Messenger_(software)",
            "Meta_(company)",
            # Google
            "Google",
            "YouTube",
            "Android",
            "Chrome",
            "Gmail",
            "Google_Maps",
            "Google_Search",
            "Google_Drive",
            "Chromebook",
            "Pixel_(smartphone)",
            "Google_Play",
            "Alphabet_Inc",
            "Google_Cloud",
            # Microsoft
            "Microsoft",
            "Windows",
            "Xbox",
            "Office_365",
            "Azure",
            "Teams_(software)",
            "Bing",
            "LinkedIn",
            "OneDrive",
            "Surface_(computer)",
            "Visual_Studio",
            "Microsoft_Office",
            "Windows_11",
            "Microsoft_365",
        ]

        # Create regex pattern for company keywords (case-insensitive)
        company_pattern = "|".join(company_keywords)
        df_filtered = df_filtered[
            df_filtered["page_title"].str.contains(
                company_pattern, case=False, na=False
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
