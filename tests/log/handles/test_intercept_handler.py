# -*- coding: utf-8 -*-
"""Contains unit-tests for the module custom python logging handlers."""
import logging
from unittest.mock import MagicMock, patch

import loguru
from hypothesis import given
from hypothesis.strategies import sampled_from
from loguru._logger import Logger

from seedspark.log.handles.intercept_handler import InterceptHandler

from ..strategies import log_record


def test_InterceptHandler_intercepts_python_logging_for_loguru(loguru_logger: Logger):
    """Ensure InterceptHandler can intercept logs from Python's logging."""

    assert InterceptHandler.add_handle(loguru_logger)

    try:

        with patch.object(loguru_logger, "info") as mocked_info:
            logging.info("test")
            assert mocked_info.called_once_with("test")
    finally:
        assert InterceptHandler.remove_handle(loguru_logger)


def test_InterceptHandler_is_handled(loguru_logger: Logger):
    """Ensure InterceptHandler is_handled works."""

    assert not InterceptHandler.is_handled(loguru_logger)

    assert InterceptHandler.add_handle(loguru_logger)
    try:
        assert InterceptHandler.is_handled(loguru_logger)
        assert InterceptHandler.remove_handle(loguru_logger)
        assert not InterceptHandler.is_handled(loguru_logger)
    finally:
        InterceptHandler.remove_handle(loguru_logger)


def test_InterceptHandler_add_handle_doesnt_add_already_handled_logger(
    loguru_logger: Logger,
):
    """Ensure InterceptHandler add_handle doesn't re-add handled logger."""

    assert InterceptHandler.add_handle(loguru_logger)
    try:
        assert not InterceptHandler.add_handle(loguru_logger)
    finally:
        assert InterceptHandler.remove_handle(loguru_logger)


@patch("logging.basicConfig")
def test_InterceptHandler_add_handle_configures_python_logging(
    mocked_basicConfig: MagicMock,
    loguru_logger: Logger,
):
    """Ensure InterceptHandler add_handle configures Python logging correctly."""

    assert InterceptHandler.add_handle(loguru_logger)
    try:
        mocked_basicConfig.assert_called_once()
        _, kwargs = mocked_basicConfig.call_args
        assert "handlers" in kwargs
        assert isinstance(kwargs["handlers"], list) and len(kwargs["handlers"]) == 1
        assert isinstance(kwargs["handlers"][0], InterceptHandler.LoggingHandler)
        assert "level" in kwargs
        assert kwargs["level"] == logging.NOTSET
    finally:
        assert InterceptHandler.remove_handle(loguru_logger)


def test_InterceptHandler_remove_handle_doesnt_remove_unhandled_logger(
    loguru_logger: Logger,
):
    """Ensure InterceptHandler remove_handler doesn't remove unhandled logger."""

    assert not InterceptHandler.is_handled(loguru_logger)
    assert not InterceptHandler.remove_handle(loguru_logger)


def test_InterceptHandler_remove_handle_restores_python_logging_configuration(
    loguru_logger: Logger,
):
    """Ensure InterceptHandler remove_handler restores default Python logging config."""

    assert InterceptHandler.add_handle(loguru_logger)
    try:
        with patch("logging.basicConfig") as mocked_basicConfig:
            assert InterceptHandler.remove_handle(loguru_logger)
            mocked_basicConfig.assert_called_with(handlers=InterceptHandler._previous_handlers)
    finally:
        InterceptHandler.remove_handle(loguru_logger)


@given(
    log_record(
        level_strategy=sampled_from(list(logging._nameToLevel.values())).filter(
            lambda x: x not in (logging.NOTSET, 5)
        )
    )
)
def test_InterceptHandler_LoggingHandler_get_level_name(record: logging.LogRecord):
    """Ensure InterceptHandler's LoggingHandler gets the appropriate level name."""

    assert InterceptHandler.LoggingHandler()._get_level_name(record) == record.levelname


@given(
    log_record(level_strategy=sampled_from([logging.NOTSET, 5])),
    sampled_from(list(loguru.logger._core.levels.keys())),  # type: ignore
)
def test_InterceptHandler_LoggingHandler_get_level_name_defaults_to_given_level_name(
    record: logging.LogRecord, default_level: str
):
    """Ensure InterceptHandler's LoggingHandler gets the default level if needed."""

    assert (
        InterceptHandler.LoggingHandler()._get_level_name(record, default_level=default_level)
        == default_level
    )
