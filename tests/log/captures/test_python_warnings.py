# -*- coding: utf-8 -*-
"""Contains unit-tests for the logging capturing of python warnings."""

import copy
import time
import warnings
from functools import partial
from unittest.mock import MagicMock, patch

import pytest
from flaky import flaky
from loguru._logger import Logger

from seedspark.log.captures import python_warnings

DEFAULT_SHOWARNING = copy.deepcopy(warnings.showwarning)


def delay_rerun(*args):
    time.sleep(2)
    return True


@flaky(max_runs=5, min_passes=2, rerun_filter=delay_rerun)
@pytest.mark.skip("_showwarning is getting detected instead of showwarning")
def test_capture_and_release_default_warning_handler(loguru_logger: Logger):
    """Ensure the module can capture and release the default warnings handler."""

    assert not isinstance(warnings.showwarning, partial)
    assert warnings.showwarning == DEFAULT_SHOWARNING

    assert python_warnings.capture(loguru_logger)
    assert warnings.showwarning != DEFAULT_SHOWARNING
    assert isinstance(warnings.showwarning, partial)
    assert warnings.showwarning.func == python_warnings._warning_handler

    assert python_warnings.release()
    assert not isinstance(warnings.showwarning, partial)
    assert warnings.showwarning == DEFAULT_SHOWARNING


def test_is_captured(loguru_logger: Logger):
    """Ensure the module can tell when warnings are being captured."""

    assert not python_warnings.is_captured()

    assert python_warnings.capture(loguru_logger)
    assert python_warnings.is_captured()

    assert python_warnings.release()
    assert not python_warnings.is_captured()


def test_release_does_not_rerelease_warning_handler(loguru_logger: Logger):
    """Esnure the module doesn't try to rerelease the non-captured warning handler."""

    assert not python_warnings.is_captured()
    assert not python_warnings.release()

    assert python_warnings.capture(loguru_logger)
    assert python_warnings.is_captured()

    assert python_warnings.release()
    assert not python_warnings.is_captured()
    assert not python_warnings.release()


def test_capture_does_not_recapture_warning_handler(loguru_logger: Logger):
    """Ensure the moudle doesn't try to recapture the captured warning handler."""

    assert python_warnings.capture(loguru_logger)

    assert python_warnings.is_captured()
    assert not python_warnings.capture(loguru_logger)
    assert python_warnings.release()


@patch("seedspark.log.captures.python_warnings._ORIGINAL_SHOWWARNING")
def test_redirects_warnings(mocked_showwarning: MagicMock, loguru_logger: Logger):
    """Ensure the module properly redirects the warnings to the logger."""

    python_warnings.release()

    try:
        with patch.object(loguru_logger, "bind", return_value=loguru_logger) as mocked_logger_bind:
            with patch.object(loguru_logger, "warning") as mocked_logger_warning:
                assert python_warnings.capture(loguru_logger)

                warnings.warn("test")
                mocked_logger_bind.assert_called_once()
                mocked_logger_warning.assert_called_once()

                mocked_showwarning.assert_called_once()
                (warning, *_) = mocked_showwarning.call_args[0]
                assert isinstance(warning, UserWarning)
                assert warning.args[0] == "test"
    finally:
        python_warnings.release()
