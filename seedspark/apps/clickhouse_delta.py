import functools
from functools import cached_property
from typing import Any, Dict, List, Optional

from loguru import logger as log

from seedspark.configs import BaseClickHouseConfig, ConfigFactory
from seedspark.connections import ClickHouse
from seedspark.sparkapp import SparkApps


# Decorator for automatic pre-start and post-stop
def spark_session_management(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        self.pre_start()
        try:
            return func(self, *args, **kwargs)
        finally:
            self.post_stop()

    return wrapper


def connection_check(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        # Check if the connection to ClickHouse is established
        if not self._check_connection():
            raise Exception("ClickHouse connection not established.")
        return func(self, *args, **kwargs)

    return wrapper


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
        # self.extra_configs = self._apply_clickhouse_config(extra_configs)
        self.extra_jars = extra_jars or []
        self.environment = environment
        self._setup_jars()

        print(f"self.extra_jars: {self.extra_jars}")
        super().__init__(
            app_name,
            extra_packages=extra_packages,
            extra_jars=self.extra_jars,
            extra_configs=extra_configs,
            spark_master=spark_master,
            log_env=log_env,
            environment=self.environment,
        )

        if clickhouse_config is None:
            self.clickhouse_configs = self.configs.clickhouse
        else:
            self.clickhouse_configs = clickhouse_config

    def _setup_jars(self):
        # clickhouse_spark_jar_url = "your_clickhouse_spark_jar_url"
        clickhouse_jdbc_jar_url = (
            "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.2/clickhouse-jdbc-0.4.2-all.jar"
        )
        self.extra_jars.extend([clickhouse_jdbc_jar_url])

    def _apply_clickhouse_config(self, configs: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        clickhouse_configs = {
            "spark.sql.catalog.clickhouse": "xenon.clickhouse.ClickHouseCatalog",
        }
        return {**clickhouse_configs, **(configs or {})}

    @cached_property  # Cache the ClickHouse connection
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
    def clickhouse_jdbc_url(self) -> str:
        """Computes the JDBC URL from the ClickHouse connection object.

        Returns:
            str: The JDBC URL.
        """
        return self.clickhouse.jdbc_url

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

    @spark_session_management
    def execute(self, query: str) -> Any:
        """Execute the given query and return the result."""
        log.info(f"Executing query: {query}")
        return self.clickhouse.sql(query)

    def post_stop(self):
        """Stop the Spark application and perform cleanup."""
        self.stop_spark_session()
        if self.clickhouse:
            self.clickhouse.disconnect()
        log.info("Post-stop cleanup completed")


# class SparkDeltaClickhouseApp(SparkApps):
#     """A high-level Spark application class for integrating Delta Lake and ClickHouse.

#     Parameters
#     ----------
#     app_name : str
#         The name of the Spark application.
#     delta_version : str
#         The version of Delta Lake to use.
#     environment : str
#         The environment for which to configure ClickHouse (local, ci, staging, prod).
#     extra_packages : Optional[List[str]]
#         Additional Maven packages required for the application.
#     extra_configs : Optional[Dict[str, Any]]
#         Additional Spark configurations.
#     spark_master : Optional[str]
#         The master URL for the Spark session.
#     log_env : bool
#         If True, logs the system environment.

#     Attributes
#     ----------
#     clickhouse_config : BaseClickHouseConfig
#         The ClickHouse configuration for the specified environment.

#     """

#     def __init__(self, app_name: str, environment: str = "local", extra_packages: Optional[List[str]] = None,
#                  extra_jars: Optional[List[str]] = None, extra_configs: Optional[Dict[str, Any]] = None,
#                  enable_delta_jar: bool = False) -> None:

#         self.configs = ConfigFactory(environment)
#         self.clickhouse_config: BaseClickHouseConfig = self.configs.clickhouse

#         # Set default extra_packages if None is provided
#         # if extra_packages is None:
#         #     self.extra_packages = ["com.clickhouse:clickhouse-jdbc:0.6.0",
#         #                            "com.github.housepower:clickhouse-spark-runtime-3.4_2.13:0.7.3"]
#         # else:
#         self.extra_packages = extra_packages

#         clickhouse_spark_jar_url = ("https://oss.sonatype.org/content/repositories/snapshots/com/github/housepower/"
#                                     "clickhouse-spark-runtime-3.5_2.13/0.8.0-SNAPSHOT/"
#                                     "clickhouse-spark-runtime-3.5_2.13-0.8.0-20240215.011503-69.jar")
#         clickhouse_jdbc_jar_url = ("https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/"
#                                     "0.4.2/clickhouse-jdbc-0.4.2-all.jar")
#         postgresql_jdbc_jar_url = ("https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/"
#                                    "postgresql-42.7.1-all.jar")
#         base_jars = [clickhouse_spark_jar_url, clickhouse_jdbc_jar_url, postgresql_jdbc_jar_url]
#         # Set default extra_jars if None is provided
#         if extra_jars is None:
#             self.extra_jars = base_jars
#         else:
#             self.extra_jars = extra_jars + base_jars

#         self.jdbc_url = None
#         self.jdbc_connection_properties = None
#         self.extra_configs = self._apply_clickhouse_config(extra_configs)

#         # Log the initialized values
#         print(
#             f"Init delta with extra configs: {self.extra_configs}, extra_jars: {self.extra_jars} and extra_packages: {self.extra_packages}")

#         super().__init__(app_name, extra_packages=self.extra_packages, extra_jars=self.extra_jars,
#                          extra_configs=self.extra_configs, enable_delta_jar=enable_delta_jar)

#     def _apply_clickhouse_config(self, configs):
#         """Applies the ClickHouse configuration to the Spark session's configurations."""
#         clickhouse_configs = {
#             "spark.sql.catalog.clickhouse": "xenon.clickhouse.ClickHouseCatalog",
#             "spark.sql.catalog.clickhouse.host": self.clickhouse_config.host,
#             "spark.sql.catalog.clickhouse.protocol": self.clickhouse_config.protocol,
#             "spark.sql.catalog.clickhouse.http_port": self.clickhouse_config.http_port,
#             "spark.sql.catalog.clickhouse.user": self.clickhouse_config.user,
#             "spark.sql.catalog.clickhouse.password": self.clickhouse_config.password,
#             "spark.sql.catalog.clickhouse.database": self.clickhouse_config.database,
#             "spark.sql.catalog.clickhouse.option.ssl": self.clickhouse_config.ssl
#         }
#         if configs is None:
#             return clickhouse_configs
#         else:
#             configs.update(clickhouse_configs)
