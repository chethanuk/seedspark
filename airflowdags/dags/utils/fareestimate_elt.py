import logging
import os
from pathlib import Path
from typing import Optional

import clickhouse_connect
import duckdb
import sqlglot
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
from sqlalchemy import create_engine


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


class FaresClickhouseToSQLPipeline:
    def __init__(
        self,
        delte_table_path="tmp/fare_kpis_raw_delta_1",
        sqllite_table_path="tmp/temp_fare_kpis.db",
    ):
        """
        Pipeline for transferring data from Clickhouse to SQLite.

        Attributes
        ----------
        logger : logging.Logger
            Logger for the class.
        clickhouse_client : clickhouse_connect.Client
            Client for connecting to Clickhouse.
        engine : sqlalchemy.engine.Engine
            Engine for connecting to SQLite.
        """
        self.logger = logging.getLogger(__name__)
        self.delta_table_path = delte_table_path
        # Split out sqllite file_name.db from path and create table
        if not os.path.exists(os.path.dirname(sqllite_table_path)):
            os.makedirs(os.path.dirname(sqllite_table_path))
        self.sqllite_table_path = sqllite_table_path

        self.clickhouse_client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "github.demo.trial.altinity.cloud"),
            port=os.getenv("CLICKHOUSE_PORT", "8443"),
            user=os.getenv("CLICKHOUSE_USER", "demo"),
            password=os.getenv("CLICKHOUSE_PASSWORD", "demo"),
            interface="https",
        )
        self.engine = create_engine(f"sqlite:///{self.sqllite_table_path}", echo=False)

    def extract_load(self, sql_file_path: Optional[str] = None):
        """
        Extract data from Clickhouse and load it into a Delta Lake raw table.

        Returns
        -------
        pd.DataFrame
            Extracted DataFrame.
        """
        self.logger.info("Starting data extraction from Clickhouse.")
        if sql_file_path is None:
            root_dir_path = Path(__file__).parent.parent.absolute().__str__()
            sql_file_path = f"{root_dir_path}/sql/fare_estimate.sql"

        sql = parse_sql_file(sql_file_path)
        self.logger.info(f"SQL file parsed successfully. SQL: {sql}")
        df = self.clickhouse_client.query_df(sql)
        print(df.head(1).to_dict(orient="records"))
        self.logger.info("Data extraction complete. Writing to Delta Lake.")

        # partition_by=["Country"],
        self._write_deltalake(self.delta_table_path, df, mode="overwrite")
        self.logger.info("Data successfully loaded into Delta Lake.")
        # Return DF count and Delta Lake path
        return {"row_count": df.shape[0], "delta_table_path": self.delta_table_path}

    def transform(self, df):
        """
        Transform the DataFrame using DuckDB to convert column names to lower and snake case.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to transform.

        Returns
        -------
        pd.DataFrame
            Transformed DataFrame.
        """
        self.logger.info(f"Column names before transformation: {df.columns}")

        # Directly modify the DataFrame's columns
        df.columns = [col.lower().replace(" ", "_") for col in df.columns]

        self.logger.info(f"Column names after transformation: {df.columns}")
        return df

    def load(self):
        """
        Load the transformed data into SQLite after a basic data quality check.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to load into SQLite.
        """
        self.logger.info("Performing basic data quality check before loading into SQLite.")

        delta_table = DeltaTable(self.delta_table_path)
        print(delta_table)
        df = delta_table.to_pandas()

        df = self.transform(df)
        row_count_delta = df.shape[0]
        print(df.head(1).to_dict(orient="records"))
        self.logger.info(f"Row count in Delta Lake: {row_count_delta}")

        self.logger.info("Loading data into SQLite.")
        with self.engine.begin() as connection:
            df.to_sql("fare_kpis", connection, if_exists="replace", index=True)

        row_count_sqlite = self.data_quality_check(sqllite_path=self.sqllite_table_path)
        assert row_count_sqlite == row_count_delta, "Row count in SQLite does not match Delta Lake."
        self.logger.info("Data loaded into SQLite successfully.")
        return {"row_count": row_count_sqlite, "sqlite_path": self.sqllite_table_path}

    def data_quality_check(self, sqllite_path="temp_fare_kpis.db"):
        """
        Perform a data quality check by verifying the row count in the SQLite database.
        """
        self.logger.info("Performing data quality check.")

        # connect to an in-memory temporary database
        conn = duckdb.connect()

        conn.execute("INSTALL sqlite; LOAD sqlite;")

        # TODO: Following seems buggy - Handle folder path and work with files properly
        sql_db = str(sqllite_path).split("/")[-1].split(".")[0]
        conn.execute(f"ATTACH '{sqllite_path}' (TYPE SQLITE); USE {sql_db};")

        # query the database as dataframe
        check_duplicates = """
        SELECT year_month,
            COUNT(year_month) AS occurrences
        FROM fare_kpis
        GROUP BY year_month
        HAVING COUNT(year_month) > 1;
        """
        df = conn.execute(check_duplicates).fetchdf()
        assert df.shape[0] == 0, "Duplicate year_month found in SQLite."

        row_count_sqlite = conn.execute("SELECT COUNT(1) FROM fare_kpis").fetchone()[0]

        self.logger.info(f"Row count in SQLite database: {row_count_sqlite}")
        conn.close()
        return row_count_sqlite

    def _write_deltalake(self, path, df, partition_by=None, mode=None):
        """
        Write DataFrame to Delta Lake.

        Parameters
        ----------
        path : str
            Path to write the Delta Lake.
        df : pd.DataFrame
            DataFrame to write.
        partition_by : list
            List of columns to partition by.
        mode : str
            Write mode.
        """
        self.logger.info(f"Writing DataFrame to Delta Lake at {path}.")
        write_deltalake(path, df, partition_by=partition_by, mode=mode)

    def run(self):
        """
        Run the ETL pipeline.
        """
        try:
            extract_dict = self.extract_load()

            load_dict = self.load()
            self.logger.info("ETL pipeline completed successfully.")
            assert (
                extract_dict["row_count"] == load_dict["row_count"]
            ), "Row count in SQLite does not match Delta Lake."

            return load_dict["row_count"]
        except Exception as e:
            self.logger.error(f"Error in ETL pipeline: {e}", exc_info=True)
            raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    pipeline = FaresClickhouseToSQLPipeline(sqllite_table_path="datasets/fare_kpis.db")
    pipeline.run()
