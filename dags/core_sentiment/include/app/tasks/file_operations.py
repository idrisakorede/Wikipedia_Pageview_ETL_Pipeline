import json
import logging
from pathlib import Path
from typing import Any, Dict

from core_sentiment.include.app_config.settings import config
from pendulum import now

logger = logging.getLogger(__name__)


def save_filtered_output(filtered_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Function:
        - Save JSON and CSV outputs to local disk.

    Args:
        - filtered_result: Dictionary with json_output and csv_output

    Returns:
        - Metadata about saved files
    """
    try:
        # Ensure processed directory exists
        processed_dir = Path(config.PROCESSED_PAGEVIEWS_DIR)
        processed_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Processed directory: {processed_dir}")

        # Generate timestamp for unique filenames
        timestamp = now("UTC").strftime("%Y%m%d_%H%M%S")

        json_path = processed_dir / f"filtered_{timestamp}.json"
        csv_path = processed_dir / f"filtered_{timestamp}.csv"

        # Validate filtered_result structure
        if not isinstance(filtered_result, dict):
            raise ValueError(f"Expected dict, got {type(filtered_result)}")

        json_output = filtered_result.get("json_output", [])
        csv_output = filtered_result.get("csv_output", "")

        # Write to JSON
        logger.info(f"Writing JSON output to: {json_path}")
        with open(json_path, "w", encoding="utf-8") as json_file:
            json.dump(json_output, json_file, indent=2, ensure_ascii=False)

        # Verify JSON file was created
        if not json_path.exists():
            raise IOError(f"Failed to create JSON file: {json_path}")
        logger.info(f"JSON file created: {json_path.stat().st_size} bytes")

        # Write to CSV
        logger.info(f"Writing CSV output to: {csv_path}")
        with open(csv_path, "w", encoding="utf-8") as csv_file:
            csv_file.write(csv_output)

        # Verify CSV file was created
        if not csv_path.exists():
            raise IOError(f"Failed to create CSV file: {csv_path}")
        logger.info(f"CSV file created: {csv_path.stat().st_size} bytes")

        result = {
            "json_file": str(json_path),
            "csv_file": str(csv_path),
            "json_records_count": len(json_output),
            "status": "success",
        }

        logger.info(f"Filtered output saved successfully: {result}")
        return result

    except (IOError, OSError) as e:
        logger.error(f"File I/O error while saving filtered output: {e}")
        raise
    except ValueError as e:
        logger.error(f"Invalid data format: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while saving filtered output: {e}")
        raise
