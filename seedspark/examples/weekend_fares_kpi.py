import os
from pathlib import Path
from typing import Optional

import sqlglot
from loguru import logger as log

from seedspark.apps import SparkDeltaClickhouseApp


def parse_sql_file(sql_file_path: str, read_dialect: str = "clickhouse", write_dialect: str = "clickhouse") -> str:
    """Parses an SQL file and returns the optimized SQL query.

    Args:
        sql_file_path: The path to the SQL file.

    Returns:
        The optimized SQL query as a string.

    Raises:
        FileNotFoundError: If the SQL file is not found.
        sqlglot.errors.ParseError: If there is an error parsing the SQL file.
    """

    if not os.path.exists(sql_file_path):
        raise FileNotFoundError(f"SQL file not found: {sql_file_path}")

    with open(sql_file_path) as f:
        query = f.read()

    try:
        sql = sqlglot.transpile(query, read=read_dialect, write=write_dialect, identify=True, pretty=True)[0]
        return sql
    except sqlglot.errors.ParseError as e:
        raise ValueError(f"Error parsing SQL file: {e.errors}") from e


class WeekendFaresKPIApp:
    """
    Class to compute weekend metrics using SparkDeltaClickhouseApp.
    """

    def __init__(self, app_name="WeekendFaresKPIApp", environment="prod", sql_file_path: Optional[str] = None):
        self.clickhouse_app = SparkDeltaClickhouseApp(
            app_name=app_name, environment=environment, extra_packages=["org.xerial:sqlite-jdbc:3.45.1.0"]
        )
        root_dir_path = Path(__file__).parent.absolute().__str__()
        if sql_file_path is None:
            sql_file_path = f"{root_dir_path}/sql/weekend_trip_metrics.sql"
        self.sql_query = parse_sql_file(sql_file_path)

    def execute(self):
        """
        Execute specific metrics calculations for weekends and return DataFrame.
        """
        # Example SQL query
        print(self.clickhouse_app.clickhouse)
        df = self.clickhouse_app.execute(self.sql_query)

        # JDBC URL for SQLite
        jdbc_url = "jdbc:sqlite:weekend_fares_kpi.db"

        # Write to SQLite using JDBC
        table_name = "weekend_fares_kpi"
        log.info(f"Writing to SQLite table: {table_name} with JDBC URL: {jdbc_url}")
        df.write.format("jdbc").option("url", jdbc_url).option("dbtable", "weekend_fares_kpi").option(
            "driver", "org.sqlite.JDBC"
        ).mode("overwrite").save()


if __name__ == "__main__":
    weekendApp = WeekendFaresKPIApp()
    weekendApp.execute()
