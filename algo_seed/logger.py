# logger.py

import logging
import sys
from pathlib import Path

def setup_logger(name):
    """
    Sets up a logger with both console and file handlers.

    Args:
        name (str): The name of the logger.

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # Set to DEBUG to capture all levels of logs

    if not logger.handlers:
        # Define log directory relative to the project's root
        # Assuming logger.py is in the project's root directory
        log_directory = Path(__file__).parent / 'logs'
        log_directory.mkdir(parents=True, exist_ok=True)  # Create logs directory if it doesn't exist

        # Define log file path
        log_file = log_directory / f"{name}.log"

        # Create file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)  # Capture all logs to the file

        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)  # Capture INFO and above to the console

        # Define log format
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
