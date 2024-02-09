import pprint
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def project_root() -> Path:
    return Path(__file__).parent.parent


@pytest.hookimpl(tryfirst=True)
def pytest_assertrepr_compare(config, op, left, right):
    if op in ("==", "!="):
        return [f"{pprint.pformat(left, width=999)} {op} {pprint.pformat(right, width=999)}"]
