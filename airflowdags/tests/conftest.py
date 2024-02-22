import logging
import os
from contextlib import contextmanager

import pytest
from airflow.models import DagBag

# Constants for tests
APPROVED_TAGS = {}
LOAD_SECOND_THRESHOLD = 3


@contextmanager
def suppress_logging(namespace):
    """
    A context manager to suppress logging for a given namespace.

    :param namespace: The logging namespace to be suppressed.
    """
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


def create_dag_bag():
    """
    Creates and returns a DagBag object, suppressing airflow logs during its creation.
    """
    with suppress_logging("airflow"):
        return DagBag(include_examples=False)


def get_import_errors():
    """
    Generates a list of tuples containing import errors in the DagBag.

    :return: List of tuples with the relative path of the DAG file and its import error message.
    """
    dag_bag = create_dag_bag()

    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

    return [(None, None)] + [(strip_path_prefix(k), v.strip()) for k, v in dag_bag.import_errors.items()]


@pytest.fixture
def get_dag_bag():
    """
    Pytest fixture to provide a DagBag object for tests.
    """
    return create_dag_bag()


def get_dags():
    """
    Generates a list of tuples containing DAG IDs, DAG objects, and their file locations from the DagBag.

    :return: List of tuples with the DAG ID, DAG object, and its file location.
    """
    dag_bag = create_dag_bag()

    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

    return [(k, v, strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]
