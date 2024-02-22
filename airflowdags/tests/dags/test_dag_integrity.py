"""
This module tests the validity of all DAGs. It ensures that each DAG has:
- Proper tags
- Retry count set to two
- No import errors
Additional tests can be added as needed.
"""

from datetime import timedelta

import pytest
from airflow.utils.dag_cycle_tester import check_cycle

#     dag_bag = create_dag_bag()
#     def strip_path_prefix(path):
#         return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))
#     return [(k, v, strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]
# Test cases below
from tests.conftest import APPROVED_TAGS, LOAD_SECOND_THRESHOLD, get_dags, get_import_errors

# # Constants for tests
# APPROVED_TAGS = {}
# LOAD_SECOND_THRESHOLD = 3
# @contextmanager
# def suppress_logging(namespace):
#     """
#     A context manager to suppress logging for a given namespace.
#     :param namespace: The logging namespace to be suppressed.
#     """
#     logger = logging.getLogger(namespace)
#     old_value = logger.disabled
#     logger.disabled = True
#     try:
#         yield
#     finally:
#         logger.disabled = old_value
# def create_dag_bag():
#     """
#     Creates and returns a DagBag object, suppressing airflow logs during its creation.
#     """
#     with suppress_logging("airflow"):
#         return DagBag(include_examples=False)
# def get_import_errors():
#     """
#     Generates a list of tuples containing import errors in the DagBag.
#     :return: List of tuples with the relative path of the DAG file and its import error message.
#     """
#     dag_bag = create_dag_bag()
#     def strip_path_prefix(path):
#         return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))
#     return [(None, None)] + [(strip_path_prefix(k), v.strip()) for k, v in dag_bag.import_errors.items()]
# @pytest.fixture
# def get_dag_bag():
#     """
#     Pytest fixture to provide a DagBag object for tests.
#     """
#     return create_dag_bag()
# def get_dags():
#     """
#     Generates a list of tuples containing DAG IDs, DAG objects, and their file locations from the DagBag.
#     :return: List of tuples with the DAG ID, DAG object, and its file location.
#     """


@pytest.mark.parametrize("rel_path,rv", get_import_errors(), ids=[x[0] for x in get_import_errors()])
def test_file_imports(rel_path, rv):
    """
    Test for import errors in DAG files.
    """
    if rel_path and rv:
        raise Exception(f"{rel_path} failed to import with message \n {rv}")


def test_dag_import_errors(get_dag_bag):
    """
    Test if a DAG contains import failures.
    :param get_dag_bag: A pytest fixture that provides a DagBag object.
    """
    dag_bag = get_dag_bag
    assert not dag_bag.import_errors, f"DAG contains import failures. Errors: {dag_bag.import_errors}"


def test_dag_loads_within_threshold(get_dag_bag):
    """
    Test if a DAGs load time is within a specified threshold.
    :param get_dag_bag: A pytest fixture that provides a DagBag object.
    """
    dag_bag = get_dag_bag
    duration = sum((dag.duration for dag in dag_bag.dagbag_stats), timedelta()).total_seconds()
    assert duration <= LOAD_SECOND_THRESHOLD, f"DAG load times exceed threshold of {LOAD_SECOND_THRESHOLD} seconds"


@pytest.mark.parametrize("dag_id,dag, fileloc", get_dags(), ids=[x[2] for x in get_dags()])
def test_dag_cycle(dag_id, dag, fileloc):
    """
    Test if a DAG has a cycle.
    :param dag_id: ID of the DAG.
    :param dag: The DAG object.
    :param fileloc: File location of the DAG.
    """
    check_cycle(dag), f"{dag_id} in {fileloc} has a cycle"


@pytest.mark.parametrize("dag_id,dag, fileloc", get_dags(), ids=[x[2] for x in get_dags()])
def test_unique_task_ids(dag_id, dag, fileloc):
    """
    Test that each task in the DAG has a unique task ID.
    :param dag_id: ID of the DAG.
    :param dag: The DAG object.
    :param fileloc: File location of the DAG.
    """
    task_ids = [task.task_id for task in dag.tasks]
    assert len(task_ids) == len(set(task_ids)), f"{dag_id} in {fileloc} - Duplicate task IDs found"


@pytest.mark.parametrize("dag_id,dag, fileloc", get_dags(), ids=[x[2] for x in get_dags()])
def test_task_id_format(dag_id, dag, fileloc):
    """
    Test that each task in the DAG has a task ID that is a valid Python identifier.
    :param dag_id: ID of the DAG.
    :param dag: The DAG object.
    :param fileloc: File location of the DAG.
    """
    from airflow.utils.task_group import TaskGroup

    for task in dag.tasks:
        if not isinstance(task, TaskGroup) and "_group." not in task.task_id:
            assert task.task_id.isidentifier(), f"{dag_id} in {fileloc} - Invalid task ID {task.task_id}"


@pytest.mark.parametrize("dag_id,dag, fileloc", get_dags(), ids=[x[2] for x in get_dags()])
def test_default_args_has_required_keys(dag_id, dag, fileloc):
    """
    Test that each DAG has a default_args dictionary with the required keys.
    :param dag_id: ID of the DAG.
    :param dag: The DAG object.
    :param fileloc: File location of the DAG.
    """
    required_keys = ["owner", "retries"]
    missing_keys = [key for key in required_keys if key not in dag.default_args]

    assert not missing_keys, f"{dag_id} in {fileloc} - Missing default args: {', '.join(missing_keys)}"


@pytest.mark.parametrize("dag_id,dag, fileloc", get_dags(), ids=[x[2] for x in get_dags()])
def test_dag_has_failure_callback(dag_id, dag, fileloc):
    """
    Test that each DAG has a default_args dictionary with the required keys.
    :param dag_id: ID of the DAG.
    :param dag: The DAG object.
    :param fileloc: File location of the DAG.
    """
    assert dag.on_failure_callback, f"{dag_id} in {fileloc} - Failure callback is not defined"


@pytest.mark.parametrize("dag_id,dag, fileloc", get_dags(), ids=[x[2] for x in get_dags()])
def test_dag_tags(dag_id, dag, fileloc):
    """
    Test if a DAG is properly tagged and if those tags are in the approved list.
    :param dag_id: ID of the DAG.
    :param dag: The DAG object.
    :param fileloc: File location of the DAG.
    """
    assert dag.tags, f"{dag_id} in {fileloc} has no tags"
    if APPROVED_TAGS:
        unapproved_tags = set(dag.tags) - set(APPROVED_TAGS)
        assert not unapproved_tags, f"{dag_id} in {fileloc} has unapproved tags: {', '.join(unapproved_tags)}"


@pytest.mark.parametrize("dag_id,dag, fileloc", get_dags(), ids=[x[2] for x in get_dags()])
def test_dag_retries(dag_id, dag, fileloc):
    """
    Test if a DAG has retries set to at least two.
    :param dag_id: ID of the DAG.
    :param dag: The DAG object.
    :param fileloc: File location of the DAG.
    """
    assert dag.default_args.get("retries", 0) >= 2, f"{dag_id} in {fileloc} does not have retries set to at least 2"


@pytest.mark.parametrize("dag_id,dag, fileloc", get_dags(), ids=[x[2] for x in get_dags()])
def test_scheduling_intervals(dag_id, dag, fileloc):
    """
    Test that each DAG has a valid scheduling interval.
    :param dag_id: ID of the DAG.
    :param dag: The DAG object.
    :param fileloc: File location of the DAG.
    """
    if dag_id not in [
        "etl_backfill",
    ]:
        assert dag.schedule_interval is not None, f"{dag_id} in {fileloc} - Missing or invalid schedule_interval"
