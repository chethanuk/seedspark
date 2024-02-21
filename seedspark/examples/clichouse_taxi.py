from onetl.db import DBReader
from onetl.strategy import SnapshotStrategy

from seedspark.apps import SparkDeltaClickhouseApp


class WeekendMetrics:
    """
    Class to compute weekend metrics using SparkDeltaClickhouseApp.
    """

    def __init__(self, app_name="WeekendMetricsApp", environment="prod"):
        self.clickhouse_app = SparkDeltaClickhouseApp(app_name=app_name, environment=environment)

    def execute(self):
        """
        Execute specific metrics calculations for weekends and return DataFrame.
        """
        # Example SQL query
        query = "SELECT AirportID, Name, City, Country, Timezone FROM airports"
        print(self.clickhouse_app.clickhouse)
        self.clickhouse_app.execute(query)


def run_snapshot_example():
    clickhouse_app = SparkDeltaClickhouseApp(
        app_name="weekend_taxi_snapshot_read", environment="local", extra_packages=["org.xerial:sqlite-jdbc:3.45.1.0"]
    )
    table = f"{clickhouse_app.clickhouse_db}.airports"
    print(f"Reading table: {table}")
    reader = DBReader(
        connection=clickhouse_app.clickhouse,
        source=table,
        # SELECT AirportID, Name, City, Country, Timezone FROM airports
        columns=["AirportID", "Name", "City", "Country", "Timezone"],
        # NOTE: hwm cannot be used with SnapshotStrategy. Only with IncrementalStrategy
        # Also NOTE: Timezone is partition column
        # hwm=DBReader.AutoDetectHWM(name="clickhouse_hwm_name", expression="Timezone"),
    )

    with SnapshotStrategy():
        print(f"Fetching all rows with SnapshotStrategy for reader: {reader}")
        df = reader.run()
        df.show(2)

        # JDBC URL for SQLite
        jdbc_url = "jdbc:sqlite:airports_database.db"

        # Write to SQLite using JDBC
        df.write.format("jdbc").option("url", jdbc_url).option("dbtable", "airports").option(
            "driver", "org.sqlite.JDBC"
        ).mode("overwrite").save()

    # with IncrementalStrategy():
    #     df = reader.run()
    #     df.show(2)
    #     print(f"df.count(): {df.count()}")


if __name__ == "__main__":
    # Example 1: Run snapshot example
    print("Running snapshot example")
    run_snapshot_example()
    # Example 2: Run weekend metrics with SQL example
    print("Running weekend metrics example")
    weekendApp = WeekendMetrics(app_name="weekend_taxi_pipeline")
    weekendApp.execute()
