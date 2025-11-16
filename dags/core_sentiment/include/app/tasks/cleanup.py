import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def cleanup_temp_files(csv_path: str) -> dict:
    """Remove temporary CSV file."""
    try:
        Path(csv_path).unlink(missing_ok=True)
        logger.info(f"✓ Cleaned up: {csv_path}")
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        return {"status": "failed"}
