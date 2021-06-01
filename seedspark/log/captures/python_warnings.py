# -*- coding: utf-8 -*-
import atexit
import copy
import warnings
from functools import partial
from typing import Optional, TextIO, Type

from loguru._logger import Logger

_ORIGINAL_SHOWWARNING = copy.deepcopy(warnings.showwarning)


def _warning_handler(
    logger: Logger,
    message: str,
    category: Type[Warning],
    filename: str,
    lineno: int,
    file: Optional[TextIO] = None,
    line: Optional[str] = None,
):
    """Substitute :func:`warnings.showwarning` method.
    .. note:: We allow the first argument to be logger by overriding the
        :func:`warnings.showwarning` method using a :class:`functools.partial` which is
        passing in the logger as the first argument.
    :param Logger logger: The logger to use to log the warning.
    :param str message: The warning message
    :param Type[~warnings.Warning] category: The category of the warning be raised
    :param str filename: The name of the file raising the warning
    :param int lineno: The line number of the file raising the warning
    :param Optional[~io.TextIO] file: The open file raising the warning, optional,
        defaults to None
    :param Optional[str] line: The line content of the open file raising the warning,
        optional, defaults to None
    """

    logger.bind(category=category, filename=filename, lineno=lineno).warning(message)
    _ORIGINAL_SHOWWARNING(message, category, filename, lineno, file, line)


def is_captured() -> bool:
    """Check if Python warnings are currently being captured.
    :return: True if warnings are captured, otherwise False
    :rtype: bool
    """

    return warnings.showwarning != _ORIGINAL_SHOWWARNING


def release() -> bool:
    """Release the current capture of Python warnings by loguru loggers.
    :returns: True if warnings are no longer being captured, False if they were not
        already being captured
    :rtype: bool
    """

    if not is_captured():
        return False

    warnings.showwarning = _ORIGINAL_SHOWWARNING
    atexit.unregister(release)
    return True


def capture(logger: Logger) -> bool:
    """Start capture of Python warnings by a given :class:`~loguru._logger.Logger`.
    :param ~loguru._logger.Logger logger: The logger to report Python warnings through
    :returns: True if warnings have started being captured, False if they are already
        being captured
    :rtype: bool
    """

    if is_captured():
        return False

    warnings.showwarning = partial(_warning_handler, logger)
    atexit.register(release)
    return True
