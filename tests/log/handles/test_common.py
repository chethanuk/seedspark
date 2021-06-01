# -*- coding: utf-8 -*-
"""Contains unit-tests for the common structure of handlers."""

import pytest

from seedspark.log.handles._common import BaseLogHandler


def test_BaseLogHandler_subclasses_requires_is_handled():
    """Ensure subclasses of BaseLogHandler requires is_handled classmethod."""

    # with pytest.raises(TypeError) as excinfo:
    #     type(
    #         "Test",
    #         (BaseLogHandler,),
    #         {"add_handle": classmethod(None), "remove_handle": classmethod(None)},
    #     )()
    #     assert "is_handled" in str(excinfo.value)

    with pytest.raises(NotImplementedError):
        type(
            "Test",
            (BaseLogHandler,),
            {"is_handled": classmethod(lambda x: BaseLogHandler.is_handled(x))},
        ).is_handled()


def test_BaseLogHandler_subclasses_requires_add_handle():
    """Ensure subclasses of BaseLogHandler requires add_handle classmethod."""

    # with pytest.raises(TypeError) as excinfo:
    #     type(
    #         "Test",
    #         (BaseLogHandler,),
    #         {"is_handled": classmethod(None), "remove_handle": classmethod(None)},
    #     )()
    #     assert "add_handle" in str(excinfo.value)

    with pytest.raises(NotImplementedError):
        type(
            "Test",
            (BaseLogHandler,),
            {"add_handle": classmethod(lambda x: BaseLogHandler.add_handle(x))},
        ).add_handle()


def test_BaseLogHandler_subclasses_requires_remove_handle():
    """Ensure subclasses of BaseLogHandler requires remove_handle classmethod."""

    # with pytest.raises(TypeError) as excinfo:
    #     type(
    #         "Test",
    #         (BaseLogHandler,),
    #         {"add_handle": classmethod(None), "is_handled": classmethod(None)},
    #     )()
    #     assert "remove_handle" in str(excinfo .value)

    with pytest.raises(NotImplementedError):
        type(
            "Test",
            (BaseLogHandler,),
            {"remove_handle": classmethod(lambda x: BaseLogHandler.remove_handle(x))},
        ).remove_handle()
