# -*- coding: utf-8 -*-
"""Contains unit-tests for the logging capturing of python exceptions."""

import copy
import sys
from functools import partial
from unittest.mock import MagicMock, patch

from loguru._logger import Logger

from seedspark.log.captures import python_exceptions

DEFAULT_EXCEPTHOOK = copy.deepcopy(sys.excepthook)


def test_capture_and_release_default_exception_handler(loguru_logger: Logger):
    """Ensure the module can capture and release the default exception handler."""

    assert not isinstance(sys.excepthook, partial)
    assert sys.excepthook == DEFAULT_EXCEPTHOOK

    assert python_exceptions.capture(loguru_logger)
    assert sys.excepthook != DEFAULT_EXCEPTHOOK
    assert isinstance(sys.excepthook, partial)
    assert sys.excepthook.func == python_exceptions._excepthook

    assert python_exceptions.release()
    assert not isinstance(sys.excepthook, partial)
    assert sys.excepthook == DEFAULT_EXCEPTHOOK


def test_is_captured(loguru_logger: Logger):
    """Ensure the module can tell when exceptions are being captured."""

    assert not python_exceptions.is_captured()

    assert python_exceptions.capture(loguru_logger)
    assert python_exceptions.is_captured()

    assert python_exceptions.release()
    assert not python_exceptions.is_captured()


def test_release_does_not_rerelease_exception_handler(loguru_logger: Logger):
    """Esnure the module doesn't try to rerelease the non-captured exception handler."""

    assert not python_exceptions.is_captured()
    assert not python_exceptions.release()

    assert python_exceptions.capture(loguru_logger)
    assert python_exceptions.is_captured()

    assert python_exceptions.release()
    assert not python_exceptions.is_captured()
    assert not python_exceptions.release()


def test_capture_does_not_recapture_exception_handler(loguru_logger: Logger):
    """Ensure the moudle doesn't try to recapture the captured exception handler."""

    assert python_exceptions.capture(loguru_logger)

    assert python_exceptions.is_captured()
    assert not python_exceptions.capture(loguru_logger)
    assert python_exceptions.release()


@patch("seedspark.log.captures.python_exceptions._ORIGINAL_EXCEPTHOOK")
def test_redirects_exceptions(mocked_excepthook: MagicMock, loguru_logger: Logger):
    """Ensure the module properly redirects the exceptions to the logger."""

    python_exceptions.release()

    try:
        with patch.object(loguru_logger, "exception") as mocked_logger_exception:
            assert python_exceptions.capture(loguru_logger)

            # We are forced to manually call sys.excepthook here as pytest
            # will remove needed references to sys.excepthook to handle their capture
            # fixtures
            exc = ValueError("test")
            sys.excepthook(type(exc), exc, exc.__traceback__)  # type: ignore

            mocked_logger_exception.assert_called_once()
            mocked_excepthook.assert_called_once()
            (exception_type, exception, *_) = mocked_excepthook.call_args[0]
            assert exception_type == ValueError
            assert isinstance(exception, ValueError)
            assert str(exception) == "test"

    finally:
        python_exceptions.release()
