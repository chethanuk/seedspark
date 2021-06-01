# -*- coding: utf-8 -*-
import abc

from loguru._logger import Logger


class BaseLogHandler(abc.ABC):
    """The abstract handler interface for how log handlers should be built."""

    @classmethod
    def is_handled(cls, logger: Logger) -> bool:
        """Quick helper method to check if the given logger is already being handled.
        :param ~loguru._logger.Logger logger: The logger to check if already handled
        :return: True if the logger is already handled, otherwise False
        :rtype: bool
        """

        raise NotImplementedError()

    @classmethod
    def add_handle(cls, logger: Logger) -> bool:
        """Add the logging handler to the given logger instance.
        :param ~loguru._logger.Logger logger: The logger instance to add the handler to
        :return: True if the handler was added, False if the handle was already present
        :rtype: bool
        """

        raise NotImplementedError()

    @classmethod
    def remove_handle(cls, logger: Logger) -> bool:
        """Remove the logging handler from the given logger instance.
        :param ~loguru._logger.Logger logger: The logger instance to remove the handle
        :return: True if the handler was removed, False if there was no handler present
        :rtype: bool
        """

        raise NotImplementedError()
