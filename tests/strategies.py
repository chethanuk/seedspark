# -*- coding: utf-8 -*-
"""Contains strategies that are useful throughout all the module tests."""

import builtins
from enum import Enum
from inspect import isclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Type

from hypothesis.strategies import (
    SearchStrategy,
    binary,
    booleans,
    builds,
    complex_numbers,
    composite,
    dictionaries,
    floats,
    from_regex,
    integers,
    lists,
    none,
    one_of,
    sampled_from,
    text,
)
from pydantic import BaseModel, create_model


class SemanticSpecOperator(Enum):
    """Enumeration of available ``SemanticSpec`` operators."""

    EQ = "=="
    NE = "!="
    LE = "<="
    GE = ">="
    LT = "<"
    GT = ">"
    EQUIV = "~="
    MAJOR = "^"
    MINOR = "~"
    NONE = ""


@composite
def builtin_types(draw, include: Optional[List[Type]] = None, exclude: Optional[List[Type]] = None) -> Any:
    """Composite strategy for building an instance of a builtin type.
    This strategy allows you to check against builtin types for when you need to do
    varaible validation (which should be rare). By default this composite will generate
    all available types of builtins, however you can either tell it to only generate
    some types or exclude some types. You do this using the ``include`` and ``exclude``
    parameters.
    For example using the ``include`` parameter like the following will ONLY generate
    strings and floats for the samples:
    >>> @given(builtin_types(include=[str, float]))
    ... def test_only_strings_and_floats(value: Union[str, float]):
    ...     assert isinstance(value, (str, float))
    Similarly, you can specify to NOT generate Nones and complex numbers like the
    following example:
    >>> @given(builtin_types(exclude=[None, complex]))
    ... def test_not_none_or_complex(value: Any):
    ...     assert value and not isinstance(value, complex)
    """

    strategies: Dict[Any, SearchStrategy[Any]] = {
        None: none(),
        int: integers(),
        bool: booleans(),
        float: floats(allow_nan=False),
        tuple: builds(tuple),
        list: builds(list),
        set: builds(set),
        frozenset: builds(frozenset),
        str: text(),
        bytes: binary(),
        complex: complex_numbers(),
    }

    to_use = set(strategies.keys())
    if include and len(include) > 0:
        to_use = set(include)

    if exclude and len(exclude) > 0:
        to_use = to_use - set(exclude)

    return draw(one_of([strategy for key, strategy in strategies.items() if key in to_use]))


@composite
def builtin_exceptions(
    draw,
    include: Optional[List[Exception]] = None,
    exclude: Optional[List[Exception]] = None,
) -> Type[Exception]:
    """Composite strategy for building a random exception."""

    to_use = set(
        [
            item
            for _, item in vars(builtins).items()
            if isclass(item) and issubclass(item, BaseException) and item not in (BaseException,)
        ]
    )

    if include and len(include) > 0:
        to_use = set(include)

    if exclude and len(exclude) > 0:
        to_use = to_use - set(exclude)

    return draw(sampled_from(list(to_use)))


@composite
def pythonic_name(draw, name_strategy: Optional[SearchStrategy[str]] = None) -> str:
    """Composite strategy for building a Python valid variable / class name."""

    return draw(from_regex(r"\A[a-zA-Z]+[a-zA-Z0-9\_]*\Z") if not name_strategy else name_strategy)


@composite
def pathlib_path(draw) -> Path:
    """Composite strategy for building a random ``pathlib.Path`` instance."""

    return Path(*draw(lists(pythonic_name(), min_size=1)))


@composite
def pydantic_model(
    draw,
    name_strategy: Optional[SearchStrategy[str]] = None,
    fields_strategy: Optional[SearchStrategy[Dict[str, Any]]] = None,
    base_class: Optional[Type[BaseModel]] = None,
) -> Type[BaseModel]:
    """Composite strategy for building a random Pydantic model."""

    return create_model(
        draw(pythonic_name() if not name_strategy else name_strategy),
        __base__=(BaseModel if not base_class else base_class),
        **draw(
            dictionaries(
                pythonic_name(),
                builtin_types(exclude=[None, set, tuple, complex, bytes]),
                min_size=1,
            )
            if not fields_strategy
            else fields_strategy
        ),
    )
