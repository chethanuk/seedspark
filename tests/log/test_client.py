# -*- coding: utf-8 -*-

"""Contains tests for the module log client."""

import sys
from unittest.mock import MagicMock, patch

import loguru
import pytest

# seedspark.
from seedspark.log.client import LOGGER_DEFAULT_CONFIG, configure_logger, get_logger, log_instance


@patch("seedspark.log.client.loguru.logger.configure")
def test_configure_logger(mocked_configure: MagicMock):
    """Ensure configure_logger configures the loguru logger."""

    configure_logger()
    mocked_configure.assert_called_once_with(**LOGGER_DEFAULT_CONFIG)


@patch("seedspark.log.client.loguru.logger.configure")
def test_configure_logger_uses_provided_logger_config(mocked_configure: MagicMock):
    """Ensure configure_logger uses the given loguru logger config."""

    config = {"handlers": {"sink": sys.stdout, "level": "DEBUG"}}
    configure_logger(config)
    mocked_configure.assert_called_once_with(**config)


def test_configure_logger_raises_valueerror_with_both_propagate_and_intercept():
    """Ensure configure_logger raises ValueError with both propagate and intercept."""

    with pytest.raises(ValueError):
        configure_logger(propagate=True, intercept=True)


@patch("seedspark.log.captures.python_warnings.capture")
@patch("seedspark.log.captures.python_warnings.release")
def test_configure_logger_captures_warnings(
    mocked_release: MagicMock,
    mocked_capture: MagicMock,
):
    """Ensure calling configure_logger with capture_warnings works."""

    configure_logger(capture_warnings=True)
    mocked_capture.assert_called_once_with(loguru.logger)
    mocked_release.assert_not_called()

    mocked_capture.reset_mock()
    configure_logger(capture_warnings=False)
    mocked_capture.assert_not_called()
    mocked_release.assert_called_once()


@patch("seedspark.log.captures.python_exceptions.capture")
@patch("seedspark.log.captures.python_exceptions.release")
def test_configure_logger_captures_exceptions(
    mocked_release: MagicMock,
    mocked_capture: MagicMock,
):
    """Ensure calling configure_logger with capture_exceptions works."""

    configure_logger(capture_exceptions=True)
    mocked_capture.assert_called_once_with(loguru.logger)
    mocked_release.assert_not_called()

    mocked_capture.reset_mock()
    configure_logger(capture_exceptions=False)
    mocked_capture.assert_not_called()
    mocked_release.assert_called_once()


@patch("seedspark.log.handles.propagate_handler.PropagateHandler.add_handle")
@patch("seedspark.log.handles.propagate_handler.PropagateHandler.remove_handle")
def test_configure_logger_propagate(mocked_remove_handle: MagicMock, mocked_add_handle: MagicMock):
    """Ensure calling configure_logger with propagate works."""

    configure_logger(propagate=True)
    mocked_add_handle.assert_called_once_with(loguru.logger)
    mocked_remove_handle.assert_not_called()

    mocked_add_handle.reset_mock()
    configure_logger(propagate=False)
    mocked_add_handle.assert_not_called()
    mocked_remove_handle.assert_called_once_with(loguru.logger)


@patch("seedspark.log.handles.intercept_handler.InterceptHandler.add_handle")
@patch("seedspark.log.handles.intercept_handler.InterceptHandler.remove_handle")
def test_configure_logger_intercept(mocked_remove_handle: MagicMock, mocked_add_handle: MagicMock):
    """Ensure calling configure_logger with intercept works."""

    configure_logger(intercept=True)
    mocked_add_handle.assert_called_once_with(loguru.logger)
    mocked_remove_handle.assert_not_called()

    mocked_add_handle.reset_mock()
    configure_logger(intercept=False)
    mocked_add_handle.assert_not_called()
    mocked_remove_handle.assert_called_once_with(loguru.logger)


def test_get_logger():
    """Ensure get_logger works."""

    get_logger.cache_clear()
    assert isinstance(get_logger(), loguru._logger.Logger)


@patch("seedspark.log.client.patch_logger")
def test_get_logger_patches_logger(mocked_patch_logger: MagicMock):
    """Ensure the logger from get_logger will attempt to patch itself."""

    get_logger.cache_clear()
    get_logger()
    mocked_patch_logger.assert_called_once_with(loguru.logger)


def test_get_logger_remaps_warn_callable():
    """Ensure the logger from get_logger contains a remapped ``warn`` callable."""

    get_logger.cache_clear()
    log = get_logger()

    assert hasattr(log, "warn")
    assert log.warn == log.warning


def test_log_instance():
    """Ensure the global log log_instance is what we expect."""

    get_logger.cache_clear()
    assert isinstance(log_instance, loguru._logger.Logger)
    assert get_logger() == log_instance
