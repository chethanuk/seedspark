import logging
import os
import random
import string
from contextlib import contextmanager
from pathlib import Path

import pytest
from airflow.models import DAG, DagBag, DagRun
from airflow.models import TaskInstance as TI
from airflow.utils import timezone
from airflow.utils.db import create_default_connections
from airflow.utils.session import create_session, provide_session

UNIQUE_HASH_SIZE = 16
MAX_TABLE_NAME_LENGTH = 62
DEFAULT_DATE = timezone.datetime(2012, 9, 2)
root_dir_path = Path(__file__).parent.parent.absolute().__str__()

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", root_dir_path)

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


@pytest.fixture()
def reset_environment():
    """
    Resets env variables.
    """
    init_env = os.environ.copy()
    yield
    changed_env = os.environ
    for key in changed_env:
        if key not in init_env:
            del os.environ[key]
        else:
            os.environ[key] = init_env[key]


# @pytest.fixture(autouse=True, scope="session")
# def mock_settings_env_vars():
#     if "AIRFLOW_HOME" in os.environ:
#         assert os.getenv("AIRFLOW_HOME") == root_dir_path
#     else:
#         with mock.patch.dict(os.environ, {"AIRFLOW_HOME": root_dir_path}):
#             yield
@pytest.fixture(autouse=True, scope="session")
def mock_settings_env_vars():
    # Save the original AIRFLOW_HOME if it exists
    original_airflow_home = os.environ.get("AIRFLOW_HOME")

    # Set the AIRFLOW_HOME to your desired path
    os.environ["AIRFLOW_HOME"] = root_dir_path

    yield

    # After the test session, restore the original AIRFLOW_HOME, if it existed
    if original_airflow_home is not None:
        os.environ["AIRFLOW_HOME"] = original_airflow_home
    else:
        del os.environ["AIRFLOW_HOME"]


# this fixture initializes the Airflow DB once per session
# it is used by DAGs in both the blogs and workflows directories,
# unless there exists a conftest at a lower level
@pytest.fixture(scope="session")
def airflow_database():
    import airflow.utils.db

    # We use separate directory for local db path per session
    # by setting AIRFLOW_HOME env var, which is done in noxfile_config.py.

    assert "AIRFLOW_HOME" in os.environ

    airflow_db = f"{AIRFLOW_HOME}/airflow.db"

    # reset both resets and initializes a new database
    airflow.utils.db.resetdb()

    # Making sure we are using a data file there.
    assert os.path.isfile(airflow_db)


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
    with suppress_logging("airflow"):
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
    with suppress_logging("airflow"):
        dag_bag = create_dag_bag()

    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

    return [(k, v, strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]


def get_dag_id(dag_id):
    """
    Generates a list of tuples containing DAG IDs, DAG objects, and their file locations from the DagBag.

    :return: List of tuples with the DAG ID, DAG object, and its file location.
    """
    with suppress_logging("airflow"):
        dag_bag = create_dag_bag()

    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

    for k, v in dag_bag.dags.items():
        if k == dag_id:
            return k, v, strip_path_prefix(v.fileloc)


@provide_session
def get_session(session=None):  # skipcq:  PYL-W0621
    create_default_connections(session)
    return session


@pytest.fixture()
def session():
    return get_session()


@pytest.fixture
def sample_dag():
    # Create Random name id
    dag_id = "".join(random.choices("test_id" + string.digits, k=UNIQUE_HASH_SIZE))
    yield DAG(dag_id, start_date=DEFAULT_DATE)
    with create_session() as session_:
        session_.query(DagRun).delete()
        session_.query(TI).delete()
