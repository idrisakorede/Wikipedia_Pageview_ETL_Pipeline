import json
import logging
from typing import Any, Dict, Hashable, List, Sequence

import pandas as pd
import requests
from core_sentiment.include.app_config.settings import config

logger = logging.getLogger(__name__)


def process_with_llm(
    prefiltered_csv_file: str, system_prompt: str, batch_size: int = 50
) -> Dict[str, Any]:
    """
    Function:
        - Main entry point: Process pre-filtered data through LLM in batches.
        - Load pre-filtered CSV
        - Split into batches for efficient processing
        - Send each batch to Ollama API
        - Aggregate filtered results
        - Return in format compatible with downstream tasks

    Args:
        prefiltered_csv_file: Path to pre-filtered CSV file
        system_prompt: System prompt with filtering rules
        batch_size: Number of records per batch (default: 50)

    Returns:
        Dictionary with:
            - json_output: List of filtered records
            - csv_output: CSV string of filtered data
            - total_records: Count of filtered records
            - statistics: Processing stats
    """
    logger.info("=" * 60)
    logger.info("LLM FILTERING WITH DIRECT OLLAMA API")
    logger.info("=" * 60)
    logger.info(f"Loading pre-filtered data: {prefiltered_csv_file}")

    # Load data
    df = pd.read_csv(prefiltered_csv_file)
    total_rows = len(df)

    logger.info(f"Total rows to process: {total_rows:,}")
    logger.info(f"Batch size: {batch_size}")
    logger.info(f"Using Ollama at: {config.OLLAMA_HOST}")
    logger.info(f"Model: {config.OLLAMA_MODEL}")

    if total_rows > 10000:
        logger.warning(f"Large dataset: {total_rows:,} rows will take time")
        estimated_batches = (total_rows + batch_size - 1) // batch_size
        estimated_time = estimated_batches * 30  # ~30 seconds per batch
        logger.warning(f"Estimated processing time: ~{estimated_time//60} minutes")

    # Process in batches
    all_filtered_results: List[Dict[str, Any]] = []
    total_batches = (total_rows + batch_size - 1) // batch_size
    successful_batches = 0
    failed_batches = 0

    for i in range(0, total_rows, batch_size):
        batch_df = df.iloc[i : i + batch_size]
        batch_num = i // batch_size + 1

        logger.info(
            f"Processing batch {batch_num}/{total_batches} ({len(batch_df)} rows)"
        )

        try:
            # Convert batch to records
            batch_records = batch_df.to_dict("records")

            # Call Ollama API
            batch_result = call_ollama_api(batch_records, system_prompt)

            # Extract and validate results
            extracted = extract_batch_results(batch_result, batch_num, len(batch_df))

            if extracted:
                all_filtered_results.extend(extracted)
                successful_batches += 1
            else:
                logger.warning(f"Batch {batch_num}: No results returned")
                failed_batches += 1

        except Exception as e:
            logger.error(f"Error processing batch {batch_num}: {e}", exc_info=True)
            failed_batches += 1
            continue

    # Generate final results
    logger.info("=" * 60)
    logger.info("BATCH PROCESSING COMPLETE")
    logger.info(f"Successful batches: {successful_batches}/{total_batches}")
    logger.info(f"Failed batches: {failed_batches}/{total_batches}")
    logger.info("=" * 60)

    return generate_final_results(
        all_filtered_results, total_rows, successful_batches, failed_batches
    )


def extract_batch_results(
    batch_result: Any, batch_num: int, batch_size: int
) -> List[Dict[str, Any]]:
    """
    Function:
        - Extract and validate results from a single batch.
        - Handles various response formats and errors gracefully.

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

    # Validate records have required fields
    if len(json_output) > 0:
        required_fields = {"domain", "page_title", "count_views"}
        sample = json_output[0]

        if not all(field in sample for field in required_fields):
            logger.error(f"Batch {batch_num}: Records missing required fields")
            logger.error(f"Expected: {required_fields}, Got: {set(sample.keys())}")
            return []

    if len(json_output) > 0:
        logger.info(
            f"Batch {batch_num}: ✓ kept {len(json_output)}/{batch_size} records"
        )
    else:
        logger.info(f"Batch {batch_num}: LLM filtered out all records")

    return json_output


def generate_final_results(
    filtered_results: List[Dict[str, Any]],
    total_input: int,
    successful_batches: int,
    failed_batches: int,
) -> Dict[str, Any]:
    """
    Function:
        - Generate final output with statistics.
        - How many records passed filtering
        - What percentage was kept
        - Batch success rate

    Args:
        filtered_results: All filtered records from all batches
        total_input: Total input records
        successful_batches: Number of successful batches
        failed_batches: Number of failed batches

    Returns:
        Dictionary with results and comprehensive metadata
    """
    if not filtered_results:
        logger.error("❌ No records passed LLM filtering!")
        logger.error("All records were filtered out or all batches failed")
        return {
            "json_output": [],
            "csv_output": "",
            "total_records": 0,
            "statistics": {
                "input_records": total_input,
                "output_records": 0,
                "filter_rate_pct": 100.0,
                "successful_batches": successful_batches,
                "failed_batches": failed_batches,
            },
        }

    # Create DataFrame from results
    final_df = pd.DataFrame(filtered_results)

    # Calculate statistics
    output_count = len(final_df)
    filter_rate = (
        ((total_input - output_count) / total_input * 100) if total_input > 0 else 0
    )
    kept_rate = (output_count / total_input * 100) if total_input > 0 else 0

    logger.info("=" * 60)
    logger.info("FINAL FILTERING RESULTS")
    logger.info("=" * 60)
    logger.info(f"Input records:    {total_input:,}")
    logger.info(f"Output records:   {output_count:,}")
    logger.info(
        f"Filtered out:     {total_input - output_count:,} ({filter_rate:.1f}%)"
    )
    logger.info(f"Kept:             {kept_rate:.1f}%")
    logger.info(f"Successful batches: {successful_batches}")
    logger.info(f"Failed batches:     {failed_batches}")
    logger.info("=" * 60)

    # Log sample of filtered pages
    logger.info("Sample of filtered pages:")
    for idx, row in enumerate(final_df.head(10).itertuples(), start=1):
        logger.info(f"  {idx}. {row.page_title}: {row.count_views:,} views")

    return {
        "json_output": filtered_results,
        "csv_output": final_df.to_csv(index=False),
        "total_records": output_count,
        "statistics": {
            "input_records": total_input,
            "output_records": output_count,
            "removed_records": total_input - output_count,
            "filter_rate_pct": round(filter_rate, 2),
            "kept_rate_pct": round(kept_rate, 2),
            "successful_batches": successful_batches,
            "failed_batches": failed_batches,
        },
    }


def create_llm_prompt(batch_records: Sequence[Dict[Hashable, Any]]) -> str:
    """
    Function:
        - Create formatted user prompt for LLM processing.

    Business Context:
        We're analyzing 5 major tech companies (Amazon, Apple, Google,
        Microsoft, Meta). The LLM needs clear instructions on what to keep
        and what to filter out.

    Args:
        batch_records: List of records to be filtered

    Returns:
        Formatted prompt string with records and instructions
    """
    user_prompt = f"""
    Analyze these {len(batch_records)} Wikipedia pageview records.

    BUSINESS CONTEXT:
    We are analyzing popularity of 5 tech companies: Amazon, Apple, Meta/Facebook, Google, Microsoft.

    TASK:
    Keep ALL genuine product and service pages.
    Remove ONLY: people, events, buildings, legal cases, historical retrospectives, controversies.

    RECORDS TO FILTER:
    {json.dumps(list(batch_records), indent=2)}

    REQUIRED OUTPUT FORMAT:
    {{
        "json_output": [
            {{"domain": "en.wikipedia.org", "page_title": "iPhone", "count_views": 50000}},
            {{"domain": "en.wikipedia.org", "page_title": "AWS", "count_views": 35000}}
        ],
        "csv_output": "domain,page_title,count_views\\nen.wikipedia.org,iPhone,50000\\n..."
    }}

    IMPORTANT: Return ONLY valid JSON. No markdown, no explanations, just the JSON object.

    """

    return user_prompt


def call_ollama_api(
    batch_records: Sequence[Dict[Hashable, Any]], system_prompt: str
) -> Dict[str, Any]:
    """
    Function:
        - Call Ollama API directly to filter a batch of records.

    Args:
        - batch_records: List of records to filter
        - system_prompt: System prompt with filtering rules

    Returns:
        - Dictionary with json_output and csv_output

    Raises:
        - Exception: If API call fails critically
    """
    # Use config values
    url = f"{config.OLLAMA_HOST}/api/generate"

    # Create user prompt with records
    user_prompt = create_llm_prompt(batch_records)
    full_prompt = f"{system_prompt}\n\n{user_prompt}"

    # Prepare API payload
    payload = {
        "model": config.OLLAMA_MODEL,  # Use config
        "prompt": full_prompt,
        "stream": False,
        "format": "json",
        "options": {
            "temperature": 0.1,  # Low temperature for consistent filtering
            "num_predict": 4000,  # Max tokens for response
        },
    }

    logger.info(
        f"Calling Ollama at {config.OLLAMA_HOST} with model {config.OLLAMA_MODEL}"
    )
    logger.info(f"Processing batch of {len(batch_records)} records...")

    try:
        response = requests.post(url, json=payload, timeout=config.OLLAMA_TIMEOUT)

        # Check HTTP status
        if response.status_code != 200:
            logger.error(f"Ollama API error: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return {"json_output": [], "csv_output": ""}

        # Parse response
        result = response.json()
        llm_response = result.get("response", "")

        if not llm_response:
            logger.error("Empty response from Ollama")
            return {"json_output": [], "csv_output": ""}

        # Log response info
        logger.info(f"LLM response length: {len(llm_response)} characters")
        logger.debug(f"LLM response preview: {llm_response[:300]}")

        # Parse JSON response
        parsed = json.loads(llm_response)

        # Validate structure
        if "json_output" not in parsed:
            logger.error("Response missing 'json_output' key")
            logger.error(f"Response keys: {list(parsed.keys())}")
            return {"json_output": [], "csv_output": ""}

        # Generate CSV if missing
        if "csv_output" not in parsed or not parsed["csv_output"]:
            logger.warning("Response missing 'csv_output' key, will generate it")
            if parsed["json_output"]:
                df = pd.DataFrame(parsed["json_output"])
                parsed["csv_output"] = df.to_csv(index=False)
            else:
                parsed["csv_output"] = ""

        # Log filtering results
        filtered_count = len(parsed["json_output"])
        input_count = len(batch_records)
        filter_rate = (
            ((input_count - filtered_count) / input_count * 100)
            if input_count > 0
            else 0
        )

        logger.info(
            f"✓ Kept {filtered_count}/{input_count} records ({100-filter_rate:.1f}% kept)"
        )

        return parsed

    except requests.exceptions.Timeout:
        logger.error(f"Ollama request timed out after {config.OLLAMA_TIMEOUT}s")
        logger.error("Consider increasing OLLAMA_TIMEOUT or reducing batch size")
        return {"json_output": [], "csv_output": ""}

    except requests.exceptions.ConnectionError as e:
        logger.error(f"Cannot connect to Ollama at {config.OLLAMA_HOST}")
        logger.error("Verify Ollama is running and accessible from Airflow")
        logger.error(f"Error details: {e}")
        return {"json_output": [], "csv_output": ""}

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse LLM response as JSON: {e}")
        logger.error(f"Raw response preview: {llm_response[:500]}")
        return {"json_output": [], "csv_output": ""}

    except Exception as e:
        logger.error(f"Unexpected error calling Ollama API: {e}", exc_info=True)
        return {"json_output": [], "csv_output": ""}


def validate_ollama_connection() -> bool:
    """
    Test Ollama connectivity before processing.

    Returns:
        True if Ollama is accessible, False otherwise
    """
    try:
        url = f"{config.OLLAMA_HOST}/api/tags"
        response = requests.get(url, timeout=5)

        if response.status_code == 200:
            models = response.json().get("models", [])
            model_names = [m.get("name") for m in models]

            logger.info(f"✓ Ollama is accessible at {config.OLLAMA_HOST}")
            logger.info(f"✓ Available models: {model_names}")

            if config.OLLAMA_MODEL not in model_names:
                logger.warning(f"⚠ Configured model '{config.OLLAMA_MODEL}' not found!")
                logger.warning(f"Available models: {model_names}")
                return False

            return True
        else:
            logger.error(f"✗ Ollama returned status {response.status_code}")
            return False

    except Exception as e:
        logger.error(f"✗ Cannot connect to Ollama: {e}")
        return False
