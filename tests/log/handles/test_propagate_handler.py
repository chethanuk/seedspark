# -*- coding: utf-8 -*-
"""Contains unit-tests for the module custom python logging handlers."""

import logging
from unittest.mock import patch

from loguru._logger import Logger

from seedspark.log.handles.propagate_handler import PropagateHandler


def test_PropagateHandler_propagates_log_records_from_loguru_to_python_logging(loguru_logger: Logger, caplog):
    """Ensure PropgateHandler can propgate messages from loguru to Python's logging."""

    assert PropagateHandler.add_handle(loguru_logger)
    try:
        loguru_logger.info("test")

        assert len(caplog.record_tuples) == 1
        _, levelno, message = caplog.record_tuples[0]
        assert levelno == logging.INFO
        assert message == "test"
    finally:
        assert PropagateHandler.remove_handle(loguru_logger)


def test_PropagateHandler_add_handle(loguru_logger: Logger):
    """Ensure PropgateHandler add_handle works."""

    assert len(PropagateHandler._handler_reference.values()) <= 0
    assert PropagateHandler.add_handle(loguru_logger)

    try:
        assert len(PropagateHandler._handler_reference.values()) == 1
    finally:
        assert PropagateHandler.remove_handle(loguru_logger)


def test_Propagatehandler_get_config():
    """Ensure PropgateHandler default config doesn't change without tests knowing."""

    config = PropagateHandler._get_config()
    assert isinstance(config["sink"], PropagateHandler.LoggingHandler)
    assert config["sink"].level == logging.DEBUG
    assert config["format"] == "{message}"


def test_PropagateHandler_get_handler_id(loguru_logger: Logger):
    """Ensure PropagateHandler can get handled logger instance's id."""

    assert PropagateHandler._get_handler_id(loguru_logger) is None
    assert PropagateHandler.add_handle(loguru_logger)

    try:
        assert isinstance(PropagateHandler._get_handler_id(loguru_logger), int)
    finally:
        assert PropagateHandler.remove_handle(loguru_logger)


def test_PropagateHandler_is_handled(loguru_logger: Logger):
    """Ensure PropagateHandler is_handled can tell when we are handling a logger."""

    assert not PropagateHandler.is_handled(loguru_logger)
    assert PropagateHandler.add_handle(loguru_logger)

    try:
        assert PropagateHandler.is_handled(loguru_logger)
    finally:
        assert PropagateHandler.remove_handle(loguru_logger)


def test_PropagateHandler_add_handle_doesnt_add_already_handled_logger(
    loguru_logger: Logger,
):
    """Ensure PropagateHandler add_handle doesn't readd handled loggers."""

    assert PropagateHandler.add_handle(loguru_logger)

    try:
        assert not PropagateHandler.add_handle(loguru_logger)
    finally:
        assert PropagateHandler.remove_handle(loguru_logger)


def test_PropagateHandler_remove_handle(loguru_logger: Logger):
    """Ensure PropagateHandler remove_handle works."""

    assert not PropagateHandler.remove_handle(loguru_logger)
    assert PropagateHandler.add_handle(loguru_logger)
    assert PropagateHandler.remove_handle(loguru_logger)


def test_PropagateHandler_remove_handle_doesnt_remove_unhandled_logger(
    loguru_logger: Logger,
):
    """Ensure PropagateHandler remove_handle does remove unhandled loggers."""

    PropagateHandler.remove_handle(loguru_logger)
    assert not PropagateHandler.remove_handle(loguru_logger)


@patch.object(PropagateHandler, "_get_handler_id")
@patch.object(PropagateHandler, "is_handled")
def test_PropagateHandler_remove_handle_doesnt_remove_logger_with_missing_handler_id(
    mocked_is_handled,
    mocked_get_handler_id,
    loguru_logger: Logger,
):
    """Ensure PropagateHandler remove_handle doesnt remove handler with no stored id."""

    mocked_is_handled.return_value = True
    mocked_get_handler_id.return_value = None

    assert not PropagateHandler.remove_handle(loguru_logger)


def test_PropagateHandler_remove_handle_passes_on_missing_handlers_in_the_logger(
    loguru_logger: Logger,
):
    """Ensure PropagateHandler remove_handle doesn't fail on missing handlers."""

    assert PropagateHandler.add_handle(loguru_logger)

    with patch.object(loguru_logger, "remove") as mocked_logger_remove:
        mocked_logger_remove.side_effect = ValueError
        assert not PropagateHandler.remove_handle(loguru_logger)


def test_PropagateHandler_remove_handle_doesnt_try_to_remove_unreferenced_loggers(
    loguru_logger: Logger,
):
    """Ensure PropagateHandler remove_handles doesn't remove unreferenced loggers."""

    assert PropagateHandler.add_handle(loguru_logger)

    try:
        with patch.object(PropagateHandler, "_handler_reference") as mocked_handler_reference:
            mocked_handler_reference.return_value = {}

            assert not PropagateHandler.remove_handle(loguru_logger)
    finally:
        PropagateHandler.remove_handle(loguru_logger)
