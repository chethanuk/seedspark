import pytest

from seedspark import SparkApps, SparkAppsException
from seedspark.dataframe import (
    DataFrameUtils,
    SchemaCompareError,
    SchemaCompareErrorType,
    SchemaComparer,
    SchemaComparerResult,
    assert_compare_data_frames,
    check_column_simple_value,
    check_column_value,
    check_struct,
    diff_lists,
    print_data_frame_info,
)
from seedspark.dataquality import AnalyzerFormat, GeValidationToFS
from seedspark.db import (
    DBReader,
    DBWriter,
    IncrementalBatchStrategy,
    IncrementalStrategy,
    Postgres,
    SnapshotBatchStrategy,
    SnapshotStrategy,
)
from seedspark.utils import WriteJsonToFS


@pytest.mark.parametrize(
    "module",
    [
        WriteJsonToFS,
        SparkAppsException,
        SparkApps,
        DBReader,
        Postgres,
        DBWriter,
        IncrementalBatchStrategy,
        IncrementalStrategy,
        SnapshotBatchStrategy,
        SnapshotStrategy,
        GeValidationToFS,
        AnalyzerFormat,
        # Dataframe
        SchemaCompareErrorType,
        SchemaCompareError,
        SchemaComparerResult,
        SchemaComparer,
        DataFrameUtils,
        check_column_value,
        diff_lists,
        assert_compare_data_frames,
        print_data_frame_info,
        check_column_value,
        check_struct,
        check_column_simple_value,
    ],
)
def test_import(module):
    imported_name = f"{module.__module__}.{module.__name__}"

    print(f"Imported name: {imported_name}")

    assert "seedspark" in imported_name, f"Imported module name '{imported_name}' doesn't not have Seedspark"
