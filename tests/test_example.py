from seedspark.contrib.testing.spark import BaseSparkTest

# pytestmark = pytest.mark.mysql


# @pytest.mark.parametrize("table", ["table", "table.table.table"])
# def test_mysql_writer_wrong_table_name(spark_mock, table):
#     mysql = MySQL(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

#     with pytest.raises(ValueError, match="Name should be passed in `schema.name` format"):
#         DBWriter(
#             connection=mysql,
#             table=table,  # Required format: table="schema.table"
#         )


# Example test case using the BaseSparkTest
class TestSparkApplication(BaseSparkTest):
    def test_spark_context(self, spark_test_fixture) -> None:
        assert spark_test_fixture.sparkContext is not None

    def test_some_spark_operation(self, spark_test_fixture) -> None:
        df = spark_test_fixture.createDataFrame([(1, "foo"), (2, "bar")], ["id", "value"])
        assert df.count() == 2
