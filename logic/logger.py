# logger.py
import logging
from datetime import datetime
from pathlib import Path


class Logger:
    """Centralized logger for the entire project."""

    def __init__(self, name="pageview_pipeline", log_level="INFO"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, log_level.upper()))

        # Prevent duplicate handlers
        if not self.logger.handlers:
            self._configure_handlers()

    def _configure_handlers(self):
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)

        # File handler
        log_file = log_dir / f"pipeline_{datetime.now().strftime('%Y-%m-%d_%H%M')}.log"
        file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def get_logger(self, module_name=None):
        """Get logger instance for a specific module."""
        if module_name:
            return logging.getLogger(f"pageview_pipeline.{module_name}")
        return self.logger


class EnhancedLogger:
    """Enhanced logger with emoji-based readability."""

    def __init__(self, logger):
        self.logger = logger

    def info(self, msg):
        self.logger.info(f"ℹ️ {msg}")

    def success(self, msg):
        self.logger.info(f"✅ {msg}")

    def warning(self, msg):
        self.logger.warning(f"⚠️ {msg}")

    def error(self, msg):
        self.logger.error(f"❌ {msg}")

    def debug(self, msg):
        self.logger.debug(f"🔍 {msg}")


# Global instance
logger_instance = Logger()


def get_logger(module_name=None):
    """Get a decorated logger instance."""
    return EnhancedLogger(logger_instance.get_logger(module_name))
