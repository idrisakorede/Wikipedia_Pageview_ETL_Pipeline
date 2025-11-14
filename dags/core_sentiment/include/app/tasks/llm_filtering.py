import json
import logging
from typing import Any, Callable, Dict, List

import pandas as pd
import requests
from core_sentiment.include.app_config.settings import config

logger = logging.getLogger(__name__)


def process_batches(
    prefiltered_csv_file: str, batch_processor_func: Callable, batch_size: int = 50
) -> Dict[str, Any]:
    """
    Function:
        - Process pre-filtered data through LLM in batches.

    Args:
        - prefiltered_csv_file: Path to pre-filtered CSV
        - batch_processor_func: Function that processes a single batch
        - batch_size: Number of rows per batch

    Returns:
        - Dictionary with filtered results and statistics
    """
    logger.info(f"Loading pre-filtered data: {prefiltered_csv_file}")
    df = pd.read_csv(prefiltered_csv_file)
    total_rows = len(df)

    logger.info(f"Total rows to process through LLM: {total_rows}")

    if total_rows > 10000:
        logger.warning(f"Large dataset: {total_rows} rows. This will take time.")
        logger.warning("Consider refining pre-filter rules if this is too slow.")

    all_filtered_results: List[Dict[str, Any]] = []
    total_batches = (total_rows + batch_size - 1) // batch_size

    for i in range(0, total_rows, batch_size):
        batch_df = df.iloc[i : i + batch_size]
        batch_num = i // batch_size + 1

        logger.info(
            f"Processing batch {batch_num}/{total_batches} ({len(batch_df)} rows)"
        )

        try:
            # Process batch through provided function
            batch_records = batch_df.to_dict("records")
            batch_result = batch_processor_func(batch_records)

            # Extract and validate results
            extracted_results = extract_batch_results(
                batch_result, batch_num, len(batch_df)
            )

            if extracted_results:
                all_filtered_results.extend(extracted_results)

        except Exception as e:
            logger.error(f"Error processing batch {batch_num}: {e}", exc_info=True)
            continue

    # Generate final results
    return generate_final_results(all_filtered_results)


def extract_batch_results(
    batch_result: Any, batch_num: int, batch_size: int
) -> List[Dict[str, Any]]:
    """
    Function:
        - Extract and validate results from a single batch.

    Args:
        - batch_result: Result from LLM processing
        - batch_num: Batch number (for logging)
        - batch_size: Original batch size

    Returns:
        - List of validated records
    """
    if batch_result is None:
        logger.warning(f"Batch {batch_num}: Result is None")
        return []

    # Handle string results (LLM sometimes returns string)
    if isinstance(batch_result, str):
        try:
            batch_result = json.loads(batch_result)
        except json.JSONDecodeError:
            logger.error(f"Batch {batch_num}: Could not parse string result as JSON")
            return []

    # Ensure it's a dict
    if not isinstance(batch_result, dict):
        logger.warning(f"Batch {batch_num}: Unexpected type {type(batch_result)}")
        return []

    # Extract json_output
    json_output = batch_result.get("json_output")

    if json_output is None:
        logger.warning(f"Batch {batch_num}: No 'json_output' key found")
        return []

    if not isinstance(json_output, list):
        logger.warning(f"Batch {batch_num}: 'json_output' is not a list")
        return []

    if len(json_output) > 0:
        logger.info(f"Batch {batch_num}: kept {len(json_output)}/{batch_size} records")
        return json_output
    else:
        logger.info(f"Batch {batch_num}: LLM filtered out all records")
        return []


def generate_final_results(filtered_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Function:
        - Generate final output

    Args:
        - filtered_results: List of all filtered records

    Returns:
        - Dictionary with results and metadata
    """
    if not filtered_results:
        logger.error("No records passed LLM filtering!")
        return {
            "json_output": [],
            "csv_output": "",
            "total_records": 0,
        }

    final_df = pd.DataFrame(filtered_results)

    logger.info("=== Final Filtering Results ===")
    logger.info(f"Total filtered records: {len(final_df)}")

    return {
        "json_output": filtered_results,
        "csv_output": final_df.to_csv(index=False),
        "total_records": len(final_df),
    }


def create_llm_prompt(batch_records: List[Dict[str, Any]]) -> str:
    """
    Function:
        - Create the user prompt for LLM processing.

    Args:
        - batch_records: List of records to filter

    Returns:
        - Formatted prompt string
    """
    user_prompt = f"""
    Analyze these {len(batch_records)} Wikipedia pageview records.

    IMPORTANT: We are analyzing popularity of 5 tech companies (Amazon, Apple, Meta/Facebook, Google, Microsoft).
    Keep ALL genuine product and service pages. Remove only: people, events, buildings, legal cases, historical retrospectives.

    Records to filter:
    {json.dumps(batch_records, indent=2)}

    Return in this format:
    {{
        "json_output": [
            {{"domain": "en.wikipedia.org", "page_title": "iPhone", "count_views": 50000}},
            ...
        ],
        "csv_output": "domain,page_title,count_views\\nen.wikipedia.org,iPhone,50000\\n..."
    }}
    """

    return user_prompt


# core_sentiment/include/src_python/llm_filtering.py


def call_ollama_api(
    batch_records: List[Dict[str, Any]], system_prompt: str
) -> Dict[str, Any]:
    """
    Function:
        - Call Ollama API directly to filter a batch of records.

    Args:
        batch_records: List of records to filter
        system_prompt: System prompt with filtering rules

    Returns:
        Dictionary with json_output and csv_output
    """
    # Use config values
    url = f"{config.OLLAMA_HOST}/api/generate"

    user_prompt = create_llm_prompt(batch_records)
    full_prompt = f"{system_prompt}\n\n{user_prompt}"

    payload = {
        "model": config.OLLAMA_MODEL,  # Use config
        "prompt": full_prompt,
        "stream": False,
        "format": "json",
        "options": {
            "temperature": 0.1,
            "num_predict": 4000,
        },
    }

    logger.info(
        f"Calling Ollama at {config.OLLAMA_HOST} with model {config.OLLAMA_MODEL}"
    )
    logger.info(f"Processing batch of {len(batch_records)} records...")

    try:
        response = requests.post(url, json=payload, timeout=config.OLLAMA_TIMEOUT)

        if response.status_code != 200:
            logger.error(f"Ollama API error: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return {"json_output": [], "csv_output": ""}

        result = response.json()
        llm_response = result.get("response", "")

        # Log response info
        logger.info(f"LLM response length: {len(llm_response)} chars")
        logger.debug(f"LLM response preview: {llm_response[:300]}")

        # Parse JSON response
        parsed = json.loads(llm_response)

        # Validate structure
        if "json_output" not in parsed:
            logger.error("Response missing 'json_output' key")
            logger.error(f"Response keys: {list(parsed.keys())}")
            return {"json_output": [], "csv_output": ""}

        if "csv_output" not in parsed:
            logger.warning("Response missing 'csv_output' key, will generate it")
            if parsed["json_output"]:
                import pandas as pd

                df = pd.DataFrame(parsed["json_output"])
                parsed["csv_output"] = df.to_csv(index=False)
            else:
                parsed["csv_output"] = ""

        filtered_count = len(parsed["json_output"])
        logger.info(f"✓ Filtered: kept {filtered_count}/{len(batch_records)} records")

        if filtered_count > 0:
            logger.debug(f"Sample filtered record: {parsed['json_output'][0]}")

        return parsed

    except requests.exceptions.Timeout:
        logger.error(f"Ollama request timed out after {config.OLLAMA_TIMEOUT}s")
        return {"json_output": [], "csv_output": ""}

    except requests.exceptions.ConnectionError as e:
        logger.error(f"Cannot connect to Ollama at {config.OLLAMA_HOST}")
        logger.error(
            "Make sure Ollama is running and accessible from Airflow container"
        )
        logger.error(f"Error: {e}")
        return {"json_output": [], "csv_output": ""}

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse LLM response as JSON: {e}")
        logger.error(f"Raw response: {llm_response[:500]}")
        return {"json_output": [], "csv_output": ""}

    except Exception as e:
        logger.error(f"Error calling Ollama API: {e}", exc_info=True)
        return {"json_output": [], "csv_output": ""}
