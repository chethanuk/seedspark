# -*- coding: utf-8 -*-
"""Contains pytest configuration for the module log tests."""

from loguru import logger
from loguru._logger import Logger
from pytest import fixture


@fixture(scope="session")
def loguru_logger() -> Logger:
    """Get the default loguru logger instance.
    :return: The default loguru logger
    :rtype: Logger
    """

    # NOTE: remove all default handlers to avoid logging to stdout during tests
    logger.configure(handlers=[])
    return logger  # type: ignore
