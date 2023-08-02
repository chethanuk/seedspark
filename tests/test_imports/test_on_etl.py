import pytest


# Test function for importing SeedSpark onETL Spark modules
@pytest.mark.parametrize(
    "module_name", ["DBReader", "DBWriter", "Postgres", "IncrementalStrategy", "SnapshotBatchStrategy"]
)
def test_imports(module_name):
    try:
        __import__(f"seedspark.spark.{module_name}")
    except ImportError:
        pytest.fail(f"Failed to import {module_name}")
