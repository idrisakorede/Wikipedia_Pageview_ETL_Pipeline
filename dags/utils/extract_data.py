# extract_data.py
import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__.split(".")[-1])

COMPANY_NAMES = [
    "Google",
    "Apple_Inc.",
    "Amazon_(company)",
    "Facebook",
    "Meta_Platforms",
    "Microsoft",
]

COMPANY_ALIASES = {
    "apple_inc.": "apple",
    "meta_platforms": "facebook",
    "alphabet": "google",
    "amazon_(company)": "amazon",
    "facebook": "facebook",
    "google": "google",
    "microsoft": "microsoft",
}


def extract_company_data(
    zipped_file: str, output_file: str = "company_views.csv"
) -> tuple[pd.DataFrame, str]:
    """
    Extract and normalize company-related Wikipedia pageviews from a gzip file.
    """
    logger.info(f"Reading and filtering gzip file in chunks: {zipped_file}")

    chunk_size = 500_000
    columns = ["domain_code", "page_title", "count_views", "total_response_size"]
    company_views = []

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
            filtered = chunk.loc[chunk["page_title"].isin(COMPANY_NAMES)].copy()
            if filtered.empty:
                continue

            filtered["page_title"] = filtered["page_title"].str.lower()
            filtered["company_name"] = filtered["page_title"].map(COMPANY_ALIASES)
            filtered = filtered.dropna(subset=["company_name"])
            company_views.append(filtered)

        if not company_views:
            logger.warning("No matching company data found.")
            return pd.DataFrame(), output_file

        result = pd.concat(company_views, ignore_index=True)
        result["count_views"] = (
            pd.to_numeric(result["count_views"], errors="coerce").fillna(0).astype(int)
        )

        result.to_csv(output_file, index=False)
        logger.success(f"Extraction completed successfully. Saved to {output_file}")
        return result, output_file

    except Exception as e:
        logger.error(f"Error during extraction: {e}")
        raise
