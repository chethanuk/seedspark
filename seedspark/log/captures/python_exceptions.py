# -*- coding: utf-8 -*-
import atexit
import copy
import sys
from functools import partial
from types import TracebackType
from typing import Type

from loguru._logger import Logger

_ORIGINAL_EXCEPTHOOK = copy.deepcopy(sys.excepthook)


def _excepthook(
    logger: Logger,
    exception_type: Type[Exception],
    exception: Exception,
    traceback: TracebackType,
):
    """Substitute :func:`sys.excepthook` method.
    .. important:: This handler will and should **always** re-call the previous
        exception hook callable as we should absolutely never silently pass on all
        exceptions raised by the Python runtime. If you do want to do something stupid
        like that, you should first override what :func:`sys.excepthook` is before
        calling :func:`~capture` from here.
    :param ~loguru._logger.Logger logger: The logger to use to log the exception
    :param Type[Exception] exception_type: The type of the exception being raised
    :param Exception exception: The instance of the exception being raised
    :param TracebackType traceback: The traceback of the exception being raised
    """

    logger.exception(exception)
    _ORIGINAL_EXCEPTHOOK(exception_type, exception, traceback)


def is_captured() -> bool:
    """Check if Python exceptions are currently being captured.
    :return: True if exceptions are being captured, otherwise False
    :rtype: bool
    """

    return sys.excepthook != _ORIGINAL_EXCEPTHOOK


def release() -> bool:
    """Release the current capture of Python exceptions by loguru loggers.
    :return: True if exceptions are no longer being captured, False if they wer not
        already being captured
    :rtype: bool
    """

    if not is_captured():
        return False

    sys.excepthook = _ORIGINAL_EXCEPTHOOK
    return True


def capture(logger: Logger) -> bool:
    """Start capture of Python exceptions by a given :class:`~loguru._logger.Logger`.
    :param ~loguru._logger.Logger logger: The logger to report Python exceptions through
    :return: True if exceptions have started being captured, False if they are already
        being captured
    :rtype: bool
    """

    if is_captured():
        return False

    sys.excepthook = partial(_excepthook, logger)
    atexit.register(release)
    return True
