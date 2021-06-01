# -*- coding: utf-8 -*-

"""Loguru Client"""
import sys
from functools import lru_cache
from typing import Optional

import loguru
from loguru._logger import Logger

# Custom logger color patterns to make things a bit prettier
from seedspark.log.captures import python_exceptions, python_warnings
from seedspark.log.handles.intercept_handler import InterceptHandler
from seedspark.log.handles.propagate_handler import PropagateHandler
from seedspark.log.patchers.patch_loggers import patch_logger

LOGGER_LEVELS = [
    {"name": "TRACE", "color": "<fg #ffffff><bg #757575>"},
    {"name": "DEBUG", "color": "<fg #757575>"},
    {"name": "INFO", "color": "<fg #2196F3>"},
    {"name": "SUCCESS", "color": "<fg #66BB6A><bold>"},
    {"name": "WARNING", "color": "<fg #FFA000>"},
    {"name": "ERROR", "color": "<fg #E53935><bold>"},
    {"name": "CRITICAL", "color": "<fg #ffffff><bg #E53935><bold>"},
]

DEFAULT_HANDLER = dict(sink=sys.stdout, level="INFO")  # noqa: C408
DEBUG_HANDLER = dict(sink=sys.stdout, level="DEBUG", backtrace=True, diagnose=True)  # noqa: C408

# The overall logging configuration we use for our application's root logger
# https://bit.ly/2NV6al8
LOGGER_DEFAULT_CONFIG = dict(levels=LOGGER_LEVELS, handlers=[DEFAULT_HANDLER])  # noqa: C408
LOGGER_DEBUG_CONFIG = dict(levels=LOGGER_LEVELS, handlers=[DEBUG_HANDLER])  # noqa: C408


def configure_logger(
    logger_config: Optional[dict] = None,
    capture_warnings: bool = False,
    capture_exceptions: bool = False,
    propagate: bool = False,
    intercept: bool = False,
):
    """Configure the root logger based on what settings we have declared.
    For additional information on what to define in the ``logger_config`` parameter,
    checkout the `Loguru configuration <https://bit.ly/2NV6al8>`_.
    .. important:: Everytime this method is called we are cycling the added handlers and
        thus their logger ids. This is required to implement the subsequent toggling of
        features that loguru handles. Because of this, the returned loguru handler ids
        are unreliable as they may be cycled by any other module which has imported this
        method.
        *In a perfect world* we should only ever configure the logger once and only once
        at the beginning of execution and toggle features using environment variables.
        Because of this, *it is recommended to avoid calling this method at all*. The
        necessary logic to configure loguru is already provided in the
        :meth:`~modist.log.get_logger` method and can be easily accessed via
        the ``instance`` property:
        >>> log.info("This is a message")
    .. caution:: This method cannot handle both propagation and interception of logs as
        doing so would result in a recursive event where all logged propagated messages
        would be intercepted and re-propagated. In reality, you should
        **rarely if ever** be using ``intercept`` as you *almost* never want to override
        how Python's default logging works. Since so much external tooling depends on
        Python's nasty logging framework, intercepting logs is seen as a bad practice.
    :param Optional[dict] logger_config: Loguru logger configuration dictionary
    :param bool capture_warnings: Logger captures all Python warnings if set to True,
        defaults to False
    :param bool capture_exceptions: Logger captures all unhandled Python exceptions if
        set to True, defaults to False
    :param bool propagate: Logger propagates all log records to Python's builtin logging
        library if set to True, defaults to False
    :param bool intercept: Logger intercepts all log records from **previously built**
        Python loggging loggers and relogs messages to the loguru logger if set to True,
        defaults to False
    :raises ValueError: When both ``propagate`` and ``intercept`` are truthy
    """

    if propagate and intercept:
        raise ValueError(
            "Cannot both propagate and intercept Python logging at the same time, "
            "this creates a circular dependency with how logging records are handled"
        )

    if not logger_config:
        logger_config = LOGGER_DEFAULT_CONFIG

    loguru.logger.configure(**logger_config)

    if capture_warnings:
        python_warnings.capture(loguru.logger)  # type: ignore
    else:
        python_warnings.release()

    if capture_exceptions:
        python_exceptions.capture(loguru.logger)  # type: ignore
    else:
        python_exceptions.release()

    # NOTE: We are forced to ignore types against loguru.logger as it performs partial
    # and renamed imports from loguru._logger.Logger as _Logger which breaks mypy
    # analysis and really can't safely be retyped in this module

    # TODO: I would like to generalize this adding and removing of handlers since this
    # code looks a little too duplicative
    if propagate:
        PropagateHandler.add_handle(loguru.logger)  # type: ignore
    else:
        PropagateHandler.remove_handle(loguru.logger)  # type: ignore

    if intercept:
        InterceptHandler.add_handle(loguru.logger)  # type: ignore
    else:
        InterceptHandler.remove_handle(loguru.logger)  # type: ignore


@lru_cache(maxsize=2)
def get_logger(log_level: str = "warn") -> Logger:
    """Get a logger instance that can be used right out of the box.
    >>> log = get_logger()
    >>> log.info("This is a message")
    2019-09-25 15:53:08.295 | INFO     | __main__:20 - This is a message
    :return: A logger instance
    :rtype: ~loguru._logger.Logger
    """

    # build a patched logger instance based on record patches we support in `patchers`
    patched_logger: Logger = patch_logger(loguru.logger)  # type: ignore

    # NOTE: loguru does not supply the deprecated (?) `warn()` alias for `warning()`.
    # So we just patch that alias in, users should utilize `warning` instead of `warn`.
    setattr(patched_logger, log_level, patched_logger.warning)

    configure_logger()
    return patched_logger


"""Instantiated logger instance that you can quickly use to log anything."""
# log: Logger = get_logger()
log_instance: Logger = get_logger()

# from ..log import log_instance as log
