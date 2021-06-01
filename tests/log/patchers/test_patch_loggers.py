# -*- coding: utf-8 -*-
"""Contains unit-tests for the :func:`~.log.patchers.patch_logger` callable."""

from unittest.mock import patch

from loguru._logger import Logger

from seedspark.log.patchers.patch_loggers import patch_logger


class _sample_patcher:
    """Dummy patcher."""

    @staticmethod
    def patch():
        pass


def test_patch_logger(loguru_logger: Logger):
    """Ensure patch_logger will apply given patches to the provided logger."""

    with patch.object(loguru_logger, "patch") as mocked_patch:
        patch_logger(loguru_logger, patchers=[_sample_patcher])
        mocked_patch.assert_called_once_with(_sample_patcher.patch)
