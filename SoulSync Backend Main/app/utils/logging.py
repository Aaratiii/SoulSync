import os
import sys
from typing import Self

from loguru import logger


class _Logger:
    _instance: Self | None = None

    @classmethod
    def init(cls):
        if cls._instance is None:
            cls._instance = _Logger()
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        # Remove default handler
        logger.remove()

        # Determine log level based on environment
        log_level = (
            "DEBUG" if os.getenv("ENV", "development") == "development" else "INFO"
        )
        log_format = "{time:YYYY-MM-DD at HH:mm:ss} - {level} - {message}"

        # Add console handler
        logger.add(
            sys.stderr,
            level=log_level,
            format=log_format,
        )

        # Add file handler
        log_file_path = "app.log"
        logger.add(
            log_file_path,
            rotation="500 MB",
            level=log_level,
            format=log_format,
        )

    def get_logger(self):
        return logger


log = _Logger.init().get_logger()
