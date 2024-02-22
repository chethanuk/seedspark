from seedspark.examples.weekend_fares_kpi import WeekendFaresKPIApp


# Example test case using the BaseSparkTest
class TestITSparkApplication:
    def test_some_spark_operation(self) -> None:
        weekendApp = WeekendFaresKPIApp()
        weekendApp.execute()

        import duckdb

        con = duckdb.connect(database="weekend_fares_kpi.db")
        print(con.execute("SELECT * FROM weekend_fares_kpi").fetchdf())
