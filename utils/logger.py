"""
This module provides a utility function to create a logger with a specified name and log level.

Dependencies:
- logging: Standard library module for logging.

Usage:
1. Call the create_logger function with the desired logger name and log level.
2. The function returns a logger object that can be used for logging messages.

Example:
    logger = create_logger(logger_name="my_logger", log_level=logging.INFO)
    logger.info("This is an info message.")
"""

import logging


def create_logger(logger_name: str, log_level=logging.INFO):
    """
    Creates a logger with the specified logger_name at the given log_level.

    Args:
        logger_name (str): Name of the logger.
        log_level (int, optional): Logging configuration level. Defaults to logging.INFO.

    Returns:
        logger: Python logger object.
    """
    # create logger
    logger = logging.getLogger(logger_name)

    # set logging level
    logger.setLevel(log_level)

    # create console handler and set level
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to console handler
    console_handler.setFormatter(formatter)

    # add console handler to logger
    logger.addHandler(console_handler)

    return logger
