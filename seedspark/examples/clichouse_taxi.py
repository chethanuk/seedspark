from onetl.db import DBReader
from onetl.strategy import SnapshotBatchStrategy

from seedspark.apps import SparkDeltaClickhouseApp
from seedspark.connections import ClickHouse


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
        df = self.clickhouse_app.execute(query)
        df.show(5)
        # print(df.schema)
        # Additional logic for weekend metrics calculations
        # return df


class WeekendMetricsOld(SparkDeltaClickhouseApp):
    """
    A high-level Spark application example class for integrating Delta Lake and ClickHouse.

    Read from ClickHouse, perform some transformations, and write to Delta Lake.
    """

    # Read weekend_trip_metrics sql from file and parse usign SQLglot to get the AST
    def execute(self):
        """
        Execute the Spark application.
        """
        # Clickhouse config
        assert self.configs.clickhouse.host == "github.demo.altinity.cloud"

        table = f"{self.clickhouse_config.database}.airports"
        clickhouse = ClickHouse(
            spark=self.spark,
            host=self.clickhouse_config.host,
            port=self.clickhouse_config.http_port,
            database=self.clickhouse_config.database,
            user=self.clickhouse_config.user,
            password=self.clickhouse_config.password,
            extra={"ssl": "true"},
        )
        print(f"clickhouse: {clickhouse} {clickhouse.port} {clickhouse.jdbc_url}")
        print(f"clickhouse: {clickhouse} {clickhouse.port} {clickhouse.instance_url}")

        clickhouse.sql("SELECT AirportID, Name, City, Country, Timezone FROM airports").show(3)
        clickhouse.check()
        print(f"clickhouse.check(): {clickhouse.check()}")

        assert clickhouse.check()
        reader = DBReader(
            connection=clickhouse,
            source=table,
            # SELECT AirportID, Name, City, Country, Timezone FROM airports
            columns=["AirportID", "Name", "City", "Country", "Timezone"],
            # Note Timezone is partition column
            hwm=DBReader.AutoDetectHWM(name="clickhouse_hwm_name", expression="Timezone"),
        )

        with SnapshotBatchStrategy(step=100) as batches:
            for i in batches:
                print(f"i: {i}")
                df = reader.run()
                df.show(2)
                df.printSchema()

        # # Read from ClickHouse
        # df = self.spark.read.format("clickhouse").options(**self.clickhouse_config.get_options()).load("trip_metrics")
        # df.createOrReplaceTempView("trip_metrics")

        # # Perform some transformations
        # weekend_metrics = self.spark.sql(self._get_weekend_metrics_sql())
        # weekend_metrics.write.format("delta").mode("overwrite").save("delta_trip_metrics")


if __name__ == "__main__":
    weekendApp = WeekendMetrics(app_name="weekend_taxi_pipeline")
    weekendApp.execute()
