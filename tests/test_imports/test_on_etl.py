import pytest

from seedspark.spark import (
    DBReader,
    DBWriter,
    IncrementalBatchStrategy,
    IncrementalStrategy,
    Postgres,
    SnapshotBatchStrategy,
    SnapshotStrategy,
)


@pytest.mark.parametrize(
    "module",
    [
        DBReader,
        Postgres,
        DBWriter,
        IncrementalBatchStrategy,
        IncrementalStrategy,
        SnapshotBatchStrategy,
        SnapshotStrategy,
    ],
)
def test_import(module):
    expected_name = f"seedspark.spark.db.onetl.{module.__name__}"
    imported_name = f"{module.__module__}.{module.__name__}"

    print(f"Expected name: {expected_name}")
    print(f"Imported name: {imported_name}")

    assert (
        imported_name == expected_name
    ), f"Imported module name '{imported_name}' doesn't match the expected name '{expected_name}'"
