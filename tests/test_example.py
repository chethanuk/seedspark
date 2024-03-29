from seedspark.contrib.testing.spark import BaseSparkTest


# Clickhouse test case using the BaseSparkTest
class TestSparkApplication(BaseSparkTest):
    def test_spark_context(self, sparkSession) -> None:
        assert sparkSession.sparkContext is not None

    def test_some_spark_operation(self, sparkSession) -> None:
        df = sparkSession.createDataFrame([(1, "foo"), (2, "bar")], ["id", "value"])
        assert df.count() == 2
