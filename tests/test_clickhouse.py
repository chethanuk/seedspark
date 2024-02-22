from seedspark.contrib.testing.spark import BaseSparkTest
from seedspark.examples.clichouse_taxi import WeekendMetrics


# Example test case using the BaseSparkTest
class TestSparkApplication(BaseSparkTest):
    def test_spark_context(self, sparkSession) -> None:
        assert sparkSession.sparkContext is not None

    def test_some_spark_operation(self, sparkSession) -> None:
        weekendApp = WeekendMetrics(app_name="weekend_taxi_pipeline")
        df = weekendApp.execute()
        assert df.count() > 2
