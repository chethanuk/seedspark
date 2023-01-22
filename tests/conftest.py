# -*- coding: utf-8 -*-
"""Contains pytest configuration and features for the module tests."""

import copy
import os
import shutil
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory, mkstemp
from typing import Any, Dict, Generator, Optional

# Import Logger object
from lognub.client import log_instance as log

from seedspark import __version__

# from pyspark.conf import SparkConf


pytest_plugins = ["helpers_namespace"]


@contextmanager
def os_environ(update_dict: Dict[str, Any]) -> Generator[os._Environ, None, None]:
    """Context manager to wrap and temporarily update the os environment.
    :param Dict[str, Any] update_dict: The keys and values to update the current os
        environment dictionary with
    """

    orig_environment = copy.copy(os.environ)
    try:
        os.environ.update(update_dict)
        yield os.environ
    finally:
        os.environ = orig_environment


@contextmanager
def cd(path: Path, create_missing: bool = True) -> Generator[Path, None, None]:
    """Context manager to ``cd`` into a given directory.
    If the path doesn't exist and ``create_missing`` is set to ``True``, this context
    manager will create the path as a directory with mode ``0o777`` and clean up the
    directory once the context is exited.
    :param Path path: The path of the directory to change to
    :param bool create_missing: If True, the given path will be created as a directory
        if one doesn't already exist
    """

    orig_path = Path.cwd()
    was_created = False
    if not path.is_dir() and create_missing:
        path.mkdir(0o777)
        log.info(f"Directory was created: {path}")
        was_created = True

    try:
        os.chdir(path.as_posix())
        yield path
    finally:
        os.chdir(orig_path.as_posix())
        if was_created:
            shutil.rmtree(path)


@contextmanager
def temporary_filepath(reason: Optional[str] = None) -> Generator[Path, None, None]:
    """Generate a temporary file inside of a context manager.
    This context manager creates a temporary file and closes it. What you get back is
    the absolute :class:`~pathlib.Path` of the created file. When the context exists,
    this created file will be deleted. So you don't get back any IO, you only get a
    location that is guaranteed to exist and be empty.
    :param Optional[str] reason: The optional reason / context for this temporary file
        existing, optional, defaults to None
    """

    suffix = f"-{__version__!s}_test"
    if isinstance(reason, str) and len(reason) > 0:
        suffix += f"-{reason!s}"

    try:
        temp_file_io, temp_file_name = mkstemp(suffix=suffix)
        os.close(temp_file_io)
        filepath = Path(temp_file_name).resolve()
        yield filepath
    finally:
        filepath.unlink()


@contextmanager
def temporary_directory(reason: Optional[str] = None) -> Generator[Path, None, None]:
    """Generate a temporary directory inside of a context manager.
    .. note:: Yes, :class:`tempfile.TemporaryDirectory` does this already. We are
        wrapping that with our own to be a bit more useful for our purpose and
        to be a bit more explict with all the directories we create for testing
    :param Optional[str] reason: The optional reason / context for this temporary
        directory existing, optional, defaults to None
    """

    suffix = f"-{__version__!s}_test"
    if isinstance(reason, str) and len(reason) > 0:
        suffix += f"-{reason!s}"

    with TemporaryDirectory(suffix=suffix) as temp_dir:
        yield Path(temp_dir).resolve()


# from tests.base_spark_test import BaseSparkTestHelper
# @pytest.fixture(scope="session")
# def spark_session(request: Any) -> SparkSession:
#     """
#     spark_session Create test's :class:`SparkSession` instance

#     Gets an existing :class:`SparkSession` or, if there is no existing one, creates a
#             new one based on the options set in this builder.

#     Parameters
#     ----------
#     request : Any
#         Calling the teardown function to be called after the last test
#         within the requesting test context finished execution

#     Returns
#     -------
#     SparkSession
#         Returns new or existing SparkSession, which is entry point to programming Spark
#         with the Dataset and DataFrame API
#     """

#     BaseSparkTestHelper.check_spark_installation()
#     BaseSparkTestHelper.clean_spark_dir()

#     # master: str = "spark://localhost:7077"
#     master = "local[4]"

#     session = (
#         SparkSession.builder.appName("pytest-pyspark-local-testing")
#         .master(master)
#         .config(conf=BaseSparkTestHelper._spark_conf())
#         .enableHiveSupport()
#         .getOrCreate()
#     )
#     BaseSparkTestHelper.quiet_py4j()

#     request.addfinalizer(lambda: BaseSparkTestHelper.clean_tear_down_session(session))
#     # If not, use runtest_protocol: pytest.hookspec.pytest_runtest_protocol
#     request.addfinalizer(lambda: BaseSparkTestHelper.clean_spark_session(session))
#     return session
