# -*- coding: utf-8 -*-
import logging
from typing import Any, Dict, Optional

from loguru._logger import Logger

from seedspark.log.handles._common import BaseLogHandler


class PropagateHandler(BaseLogHandler):
    """Propagate Loguru's logging records to Python's builtin logging.
    You will probaly want to utilize this handler in order to have your loguru log
    records being logged over Python's builtin logging API for better compatibility with
    external tools that explicitly monitor Python's builtin logging
    (such as Sentry, Prometheus, etc.).
    """

    _handler_reference: Dict[Any, int] = {}

    class LoggingHandler(logging.Handler):
        """Log handler that propagates Loguru logs to Python's builtin logging."""

        def handle(self, record: logging.LogRecord):
            """Given a :class:`logging.LogRecord`, handle it with Python logging.
            :param ~logging.LogRecord record: The log record to handle
            """

            logging.getLogger(record.name).handle(record)

    @staticmethod
    def _get_config() -> Dict[str, Any]:
        """Get the loguru configuration dictionary that should be used for this handler.
        :return: The configuration dictionary to add to loguru's logger, or None
        :rtype: Dict[str, Any]
        """

        return dict(  # noqa: C408
            sink=PropagateHandler.LoggingHandler(level=logging.DEBUG),
            format="{message}",  # noqa: FS003
        )

    @classmethod
    def _get_handler_id(cls, logger: Logger) -> Optional[int]:
        """Get the handler's id for the given logger.
        :param ~loguru._logger.Logger logger: The logger to lookup the handler id for
        :return: The integer id of the given logger's handler, otherwise None
        :rtype: Optional[int]
        """

        if logger in cls._handler_reference:
            return cls._handler_reference[logger]

        return None

    @classmethod
    def is_handled(cls, logger: Logger) -> bool:
        """Quick helper method to check if the given logger is already being handled.
        :param ~loguru._logger.Logger logger: The logger to check if already handled
        :return: True if the logger is already handled, otherwise False
        :rtype: bool
        """

        return cls._get_handler_id(logger) is not None

    @classmethod
    def add_handle(cls, logger: Logger) -> bool:
        """Add the logging handler to the given logger instance.
        :param ~loguru._logger.Logger logger: The logger instance to add the handler to
        :return: True if the handler was added, False if the handle was already present
        :rtype: bool
        """

        if cls.is_handled(logger):
            return False

        handler_id: int = logger.add(**PropagateHandler._get_config())
        cls._handler_reference[logger] = handler_id

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

        handler_id = cls._get_handler_id(logger)
        try:
            if not handler_id:
                return False

            logger.remove(handler_id)
            return True
        except ValueError:
            # NOTE: this occurs in an edge-case where the logger is intialized and a
            # PropagateHandler.Logginghandler is added manually
            # (not using the included methods).
            #
            # This doesn't really cause any issues in normal execution, just that the
            # `handler_id` won't exist in our references. Another reason to only
            # configure the loguru logger only once at startup and avoid doing
            # subsequent calls to `modist.log.configure_logger`.
            return False
        finally:
            if logger in cls._handler_reference:
                del cls._handler_reference[logger]
