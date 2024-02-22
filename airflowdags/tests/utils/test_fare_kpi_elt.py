import pandas as pd
import pytest
from deltalake import DeltaTable
from utils.fareestimate_elt import FaresClickhouseToSQLPipeline

# Sample Clickhouse data
clickhouse_data = [
    {
        "year_month": "2014-01",
        "sat_avg_trip_count": 491563.8,
        "sat_avg_fare_per_trip": 11.4,
        "sat_avg_trip_duration": 11.3,
        "sun_avg_trip_count": 439573.2,
        "sun_avg_fare_per_trip": 12.0,
        "sun_avg_trip_duration": 11.0,
    }
]

# Edge case data
edge_case_data = [
    {
        "year_month": " ",
        "sat_avg_trip_count": -491563.8,
        "sat_avg_fare_per_trip": -11.4,
        "sat_avg_trip_duration": 0,
        "sun_avg_trip_count": -439573.2,
        "sun_avg_fare_per_trip": 12.0,
        "sun_avg_trip_duration": 11.0,
    }
]

DELTA_TABLE_PATH = "fares_raw"
SQLLITE_TABLE_PATH = "temp_fares.db"


@pytest.fixture
def pipeline_instance(tmp_path):
    return FaresClickhouseToSQLPipeline(
        delte_table_path=tmp_path / DELTA_TABLE_PATH,
        sqllite_table_path=tmp_path / SQLLITE_TABLE_PATH,
    )


def test_write_deltalake(pipeline_instance, tmp_path):
    # Call the method to test
    extract_dict = pipeline_instance.extract_load()
    row_count = extract_dict["row_count"]
    delta_table_path = extract_dict["delta_table_path"]

    # Check if Delta Lake files are created
    delta_path = tmp_path / DELTA_TABLE_PATH
    assert str(delta_table_path) == str(delta_path), "Delta Lake path does not match expected path"
    assert delta_path.exists(), "Delta Lake path does not exist"

    # Read back data from Delta Lake and verify its contents
    delta_table = DeltaTable(str(delta_path))
    delta_df = delta_table.to_pandas()
    assert row_count == delta_df.shape[0], "Data in Delta Lake does not match original DataFrame"


# Test transform
@pytest.mark.parametrize("test_input", [(pd.DataFrame(clickhouse_data)), (pd.DataFrame(edge_case_data))])
def test_transform(pipeline_instance, test_input):
    result = pipeline_instance.transform(test_input.copy())
    # Adjust expected columns based on what your transform method should produce
    expected_columns = [
        "year_month",
        "sat_avg_trip_count",
        "sat_avg_fare_per_trip",
        "sat_avg_trip_duration",
        "sun_avg_trip_count",
        "sun_avg_fare_per_trip",
        "sun_avg_trip_duration",
    ]
    assert list(result.columns) == expected_columns


def test_pipeline_run(pipeline_instance, tmp_path):
    result = pipeline_instance.run()

    assert result > 0, "Pipeline run failed"

    sqllite_cnt = pipeline_instance.data_quality_check(sqllite_path=tmp_path / SQLLITE_TABLE_PATH)

    assert sqllite_cnt == result, "Row count in SQLite does not match Delta Lake."
