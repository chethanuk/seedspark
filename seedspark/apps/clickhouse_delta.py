#
from functools import cached_property
from typing import Any, Dict, List, Optional

from loguru import logger as log

from seedspark.configs import BaseClickHouseConfig, ConfigFactory
from seedspark.connections import ClickHouse
from seedspark.decorators import connection_check, log_exceptions, spark_session_management
from seedspark.sparkapp import SparkApps


class SparkDeltaClickhouseApp(SparkApps):
    def __init__(  # noqa: PLR0913
        self,
        app_name: str,
        clickhouse_config: BaseClickHouseConfig = None,
        extra_packages: Optional[List[str]] = None,
        extra_configs: Optional[Dict[str, Any]] = None,
        extra_jars: Optional[List[str]] = None,
        spark_master: Optional[str] = None,
        environment="staging",
        log_env: bool = True,
    ):
        self.extra_configs = {**(self._apply_delta_config()), **(extra_configs or {})}
        # self._apply_clickhouse_config(extra_configs)
        # Ensure extra_jars is a list of strings
        self.extra_jars = extra_jars or []
        self.environment = environment
        self._setup_jars()

        print(f"self.extra_jars: {self.extra_jars}")
        print(f"self.extra_configs: {self.extra_configs}")
        super().__init__(
            app_name,
            extra_packages=extra_packages,
            extra_jars=self.extra_jars,
            extra_configs=self.extra_configs,
            spark_master=spark_master,
            log_env=log_env,
            environment=self.environment,
        )
        print(self.spark.version)

        if clickhouse_config is None:
            self.clickhouse_configs = self.configs.clickhouse
        else:
            self.clickhouse_configs = clickhouse_config

    @log_exceptions
    def _setup_jars(self):
        # clickhouse_spark_jar_url = "your_clickhouse_spark_jar_url"
        clickhouse_jdbc_jar_url = (
            "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.0/clickhouse-jdbc-0.6.0-all.jar"
        )
        self.extra_jars.extend([clickhouse_jdbc_jar_url])

    def _apply_clickhouse_config(self, clickhouse_configs: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        _catalog_configs = {
            "spark.sql.catalog.clickhouse": "xenon.clickhouse.ClickHouseCatalog",
            "spark.sql.catalog.clickhouse.host": self.clickhouse_config.host,
            "spark.sql.catalog.clickhouse.protocol": self.clickhouse_config.protocol,
            "spark.sql.catalog.clickhouse.http_port": self.clickhouse_config.http_port,
            "spark.sql.catalog.clickhouse.user": self.clickhouse_config.user,
            "spark.sql.catalog.clickhouse.password": self.clickhouse_config.password,
            "spark.sql.catalog.clickhouse.database": self.clickhouse_config.database,
            "spark.sql.catalog.clickhouse.option.ssl": self.clickhouse_config.ssl,
        }
        return {**_catalog_configs, **(clickhouse_configs or {})}

    def _apply_delta_config(self, delta_configs: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Applies Delta configurations to the provided dictionary.

        Args:
          delta_configs: A dictionary containing Delta configurations to be applied.

        Returns:
          A dictionary with combined catalog configurations and optional user-provided configurations.
        """
        _catalog_configs = {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        }
        return {**_catalog_configs, **(delta_configs or {})}

    @property  # Cache the ClickHouse connection
    def clickhouse(self) -> ClickHouse:
        """Establishes a connection to ClickHouse.

        Returns:
            ClickHouseConnection: A connection object.
        """
        clickhouse = ClickHouse(
            spark=self.spark,
            host=self.clickhouse_config.host,
            port=self.clickhouse_config.http_port,
            user=self.clickhouse_config.user,
            password=self.clickhouse_config.password,
            database=self.clickhouse_config.database,
            extra={"ssl": str(self.clickhouse_config.ssl).lower()},
        )

        print(f"clickhouse: {clickhouse}")
        return clickhouse

    @cached_property
    def configs(self) -> ConfigFactory:  # Type hint can clarify
        return ConfigFactory(self.environment)  # Assumes environment remains consistent

    @cached_property
    def clickhouse_config(self) -> BaseClickHouseConfig:
        return self.configs.clickhouse

    @cached_property
    def clickhouse_db(self) -> BaseClickHouseConfig:
        return self.configs.clickhouse.database

    @cached_property
    def clickhouse_jdbc_url(self) -> str:
        """Computes the JDBC URL from the ClickHouse connection object.

        Returns:
            str: The JDBC URL.
        """
        return self.clickhouse.jdbc_url

    @cached_property
    @log_exceptions
    def clickhouse_reader(self, table, columns, partition_col):
        from onetl.db import DBReader

        return DBReader(
            connection=self.clickhouse,
            source=table,
            columns=columns,
            hwm=DBReader.AutoDetectHWM(name=f"clickhouse_{table}_hwm_name", expression=partition_col),
        )

    def pre_start(self):
        """
        Initialize configurations and check ClickHouse connection.
        """
        # self._init_clickhouse_client()
        if not self._check_connection():
            raise ConnectionError(f"Failed to connect to ClickHouse: {self.clickhouse}")

    def _check_connection(self) -> bool:
        """Check the ClickHouse database connection."""
        try:
            self.clickhouse.check()
            log.info(f"ClickHouse connection successful for url: {self.clickhouse_jdbc_url}")
            return True
        except Exception as e:
            log.error(f"ClickHouse connection check failed: {e}")
            return False

    @log_exceptions
    @spark_session_management
    @connection_check
    def execute(self, query: str) -> Any:
        log.info(f"Executing query in Clickhouse DB: {query}")
        df = self.clickhouse.sql(query)
        log.info(f"Schema of clickhouse dataframe: {df.schema}")
        df.show(5)
        return df

    @log_exceptions
    def post_stop(self):
        """Stop the Spark application and perform cleanup.
        NOTE: This is not working as expected
        Because Spark work asynchronusly, the cleanup is not working as expected. TODO: Fix this."""
        # super().stop_spark_session()
        # if self.clickhouse.check():
        #     self.clickhouse.close()
        log.info("Post-stop cleanup completed")
