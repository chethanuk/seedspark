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

    def __init__(
        self,
        app_name="WeekendFaresKPIApp",
        environment="prod",
        sql_file_path: Optional[str] = None,
        sqllite_db_path: Optional[str] = None,
    ):
        self.clickhouse_app = SparkDeltaClickhouseApp(
            app_name=app_name, environment=environment, extra_packages=["org.xerial:sqlite-jdbc:3.45.1.0"]
        )
        root_dir_path = Path(__file__).parent.absolute().__str__()
        if sql_file_path is None:
            sql_file_path = f"{root_dir_path}/sql/weekend_trip_metrics.sql"
        self.sql_query = parse_sql_file(sql_file_path)

        # JDBC URL for SQLite
        if sqllite_db_path is None:
            sqllite_db_path = "weekend_fares_kpi.db"
        self.sqlite_jdbc_url = f"jdbc:sqlite:{sqllite_db_path}"
        self.table_name = "weekend_fares_kpi"

    def execute(self):
        """
        Execute specific metrics calculations for weekends and return DataFrame.
        """
        # Example SQL query
        df = self.clickhouse_app.execute(self.sql_query)

        # Write to SQLite using JDBC
        log.info(f"Writing to SQLite table: {self.table_name} with JDBC URL: {self.sqlite_jdbc_url}")
        df.write.format("jdbc").option("url", self.sqlite_jdbc_url).option("dbtable", self.table_name).option(
            "driver", "org.sqlite.JDBC"
        ).mode("overwrite").save()


if __name__ == "__main__":
    weekendApp = WeekendFaresKPIApp()
    weekendApp.execute()
