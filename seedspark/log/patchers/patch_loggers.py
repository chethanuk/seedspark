# -*- coding: utf-8 -*-
"""Module containing loguru log record patchers."""

from typing import Any, Iterable

from loguru._logger import Logger

__all__ = ["patch_logger"]

# An interable of patchers we utilize by default for our root `get_logger()` method
# and our `instance` attribute
DEFAULT_PATCHERS: Iterable[Any] = []


def patch_logger(logger: Logger, patchers: Iterable[Any] = DEFAULT_PATCHERS) -> Logger:
    """Produce a new *wrapping* logger that has been patched with the given patchers.
    .. note:: You will probably never need to utilize this method to manually patch
        A logger instance with a specific patcher. This is simply a helper method
        to auto-apply patches required for the application root logger.
        Likely all you need to use to begin logging is to utilize the provided logger
        instance:
        .. code-block:: python
            from modist.log import instance as log
            log.info("This is a logged message")
    Usage of this method typically looks something like the following:
    :param ~loguru._logger.Logger logger: The root logger instance to patch
    :param Iterable[Any] patchers: A list of patcher modules to use when producing a
        new patched logger instsance, optional, defaults to DEFAULT_PATCHERS
    :return: A new patched logger instance
    :rtype: ~loguru._logger.Logger
    """

    patched_logger: Logger = logger

    for patcher in patchers:
        patched_logger = patched_logger.patch(patcher.patch)

    return patched_logger
