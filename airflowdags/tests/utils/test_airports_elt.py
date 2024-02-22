import pandas as pd
import pytest
from deltalake import DeltaTable
from utils.airports_elt import ClickhouseToSQLitePipeline

# Sample Clickhouse data
clickhouse_data = [
    {
        "AirportID": "1",
        "Name": "Goroka Airport",
        "City": "Goroka",
        "Country": "Papua New Guinea",
        "IATA": "GKA",
        "ICAO": "AYGA",
        "Latitude": -6.081689834594727,
        "Longitude": 145.39199829101562,
        "Altitude": 5282,
        "Timezone": 10.0,
        "DST": "U",
        "Tz": "Pacific/Port_Moresby",
        "Type": "airport",
        "Source": "OurAirports",
    }
]

# Edge case data
edge_case_data = [
    {
        "AirportID": "1",
        "Name": " ",
        "city": "Goroka",
        "Country": None,
        "IATA": "GKA",
        "ICAO": "AYGA",
        "Latitude": -6.081689834594727,
        "Longitude": 145.39199829101562,
        "Altitude": 5282,
        "Timezone": -10.0,
        "DsT": "U",
        "Tz": "Pacific/Port_Moresby",
        "Type": "airport",
        "Source": "OurAirports",
    }
]


@pytest.fixture
def pipeline_instance(tmp_path):
    return ClickhouseToSQLitePipeline(
        delte_table_path=tmp_path / "delta",
        sqllite_table_path=tmp_path / "temp_airports.db",
    )


def test_write_deltalake(pipeline_instance, tmp_path):
    # Call the method to test
    extract_dict = pipeline_instance.extract_load()
    row_count = extract_dict["row_count"]
    delta_table_path = extract_dict["delta_table_path"]

    # Check if Delta Lake files are created
    delta_path = tmp_path / "delta"
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
        "airportid",
        "name",
        "city",
        "country",
        "iata",
        "icao",
        "latitude",
        "longitude",
        "altitude",
        "timezone",
        "dst",
        "tz",
        "type",
        "source",
    ]
    assert list(result.columns) == expected_columns


def test_pipeline_run(pipeline_instance, tmp_path):
    result = pipeline_instance.run()

    assert result > 0, "Pipeline run failed"

    sqllite_cnt = pipeline_instance.data_quality_check(sqllite_path=tmp_path / "temp_airports.db")

    assert sqllite_cnt == result, "Row count in SQLite does not match Delta Lake."
