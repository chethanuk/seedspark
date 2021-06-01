# -*- coding: utf-8 -*-
import copy
import logging
from functools import lru_cache
from typing import Any, List

import loguru
from loguru._logger import Logger

from seedspark.log.handles._common import BaseLogHandler


class InterceptHandler(BaseLogHandler):
    """Intercept Python's builtin logging as Loguru logging records.
    You will probably want to use this when you wish another module's logging to passed
    into whatever Loguru sinks you have configured. This handler was originally added
    for HTTP API frameworks such as werkzeug's logging needing to be passed through into
    whatever sinks are configured by the global loguru logging instance.
    .. important:: This handler intercepts all **already defined** builtin logger logs
        through loguru's log record handling. This means that builtin loggers defined
        **after** this handler is added will not be captured (and will likely break the
        configuration we do to support logging intercepts). So if you need to use
        this handler, do your absolute best to ensure that the call to
        :meth:`~InterceptHandler.add_handle` is performed after other module using
        Python's builtin logging are imported and configured.
    Some example usage of what using this handler looks like is included below:
    >>> log = get_logger()
    >>> from seedspark.log.handles.intercept_handler import InterceptHandler
    >>> InterceptHandler.add_handle(log)
    .. caution:: Python log intercepting is not very robust or dynamic. You bascially
        need to decided if you want to intercept all logs or if you want to ignore other
        module's log's at the very start of runtime. You can't update constructed
        loggers from Python's builtin :mod:`logging` after they have been defined.
        Also any third-party module that calls :func:`logging.basicConfig` and resets
        global logging handlers will break this handler. Your best course of action is
        to just accept that Python's logging will always suck and only worry about your
        own modules logging.
    """

    _is_intercepting: bool = False
    _previous_handlers: List[Any] = []

    class LoggingHandler(logging.Handler):
        """Log handler that intercepts Python's builtin logging for Loguru logs."""

        @lru_cache()
        def _get_level_name(self, record: logging.LogRecord, default_level: str = "INFO") -> str:
            """Determine the loguru record name for the given Python logging record.
            :param logging.LogRecord record: The record to determine the level name from
            :return: The logging level name for Loguru
            :rtype: str
            """

            valid_names = list(loguru.logger._core.levels.keys())  # type: ignore
            if record.levelname in valid_names:
                return record.levelname

            return default_level

        def emit(self, record: logging.LogRecord):  # pragma: no cover
            """Given a :class:`logging.LogRecord`, handle it with Loguru logging.
            :param ~logging.LogRecord record: The log record to handle
            """

            frame, depth = logging.currentframe(), 2  # type: ignore
            while frame.f_code.co_filename == logging.__file__:
                if frame.f_back is not None:
                    frame = frame.f_back
                depth += 1

            loguru.logger.opt(depth=depth, exception=record.exc_info).log(
                self._get_level_name(record), record.getMessage()
            )

    @classmethod
    def is_handled(cls, logger: Logger) -> bool:
        """Quick helper method to check if the given logger is already being handled.
        :param ~loguru._logger.Logger logger: The logger to check if already handled
        :return: True if the logger is already handled, otherwise False
        :rtype: bool
        """

        return cls._is_intercepting

    @classmethod
    def add_handle(cls, logger: Logger) -> bool:
        """Add the logging handler to the given logger instance.
        :param ~loguru._logger.Logger logger: The logger instance to add the handler to
        :return: True if the handler was added, False if the handle was already present
        :rtype: bool
        """

        if cls.is_handled(logger):
            return False

        cls._previous_handlers = copy.copy(logging._handlerList)  # type: ignore
        logging.basicConfig(handlers=[cls.LoggingHandler()], level=logging.NOTSET)
        cls._is_intercepting = True
        return True

    @classmethod
    def remove_handle(cls, logger: Logger) -> bool:
        """Remove the logging handler from the given logger instance.
        :param ~loguru._logger.Logger logger: The logger instance to remove the handle
        :return: True if the handler was removed, False if there was no handler present
        :rtype: bool
        """

        if not cls.is_handled(logger):
            return False

        logging.basicConfig(handlers=cls._previous_handlers)
        cls._is_intercepting = False
        return True
