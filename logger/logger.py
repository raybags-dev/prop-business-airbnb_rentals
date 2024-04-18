import logging
import traceback
from pathlib import Path
from colorama import init, Fore, Style


LOG_FOLDER = "logs"
LOG_FILENAMES = {"info": "info.log", "error": "error.log", "warn": "warn.log"}


def initialize_logging() -> None:
    Path(LOG_FOLDER).mkdir(parents=True, exist_ok=True)
    for log_type, filename in LOG_FILENAMES.items():
        logger = logging.getLogger(log_type)
        if log_type == "info":
            logger.setLevel(logging.INFO)
        elif log_type == "error":
            logger.setLevel(logging.ERROR)
        else:
            logger.setLevel(logging.WARNING)

        handler = logging.FileHandler(Path(LOG_FOLDER) / filename)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)


def my_log(message, log_type="info"):
    if log_type not in LOG_FILENAMES:
        raise ValueError("Invalid log type. Supported types: info, error, warn")

    logger = logging.getLogger(log_type)
    if log_type == "info":
        logger.info(message)
    elif log_type == "error":
        traceback_message = traceback.format_exc()
        logger.error(f"{message}\n{traceback_message}")
    else:
        logger.warning(message)

