from typing import TYPE_CHECKING, Any, Dict, List, Optional

from seedspark import sparkapp
from seedspark.configs.configs import ConfigFactory

if TYPE_CHECKING:
    from seedspark.configs.clickhouse import BaseClickHouseConfig


class SparkDeltaClickhouseApp(sparkapp):
    """A high-level Spark application class for integrating Delta Lake and ClickHouse.

    Parameters
    ----------
    app_name : str
        The name of the Spark application.
    delta_version : str
        The version of Delta Lake to use.
    environment : str
        The environment for which to configure ClickHouse (local, ci, staging, prod).
    extra_packages : Optional[List[str]]
        Additional Maven packages required for the application.
    extra_configs : Optional[Dict[str, Any]]
        Additional Spark configurations.
    spark_master : Optional[str]
        The master URL for the Spark session.
    log_env : bool
        If True, logs the system environment.

    Attributes
    ----------
    clickhouse_config : BaseClickHouseConfig
        The ClickHouse configuration for the specified environment.

    """

    def __init__(  # noqa: PLR0913
        self,
        app_name: str,
        delta_version: str,
        environment: str = "local",
        extra_packages: Optional[List[str]] = None,
        extra_configs: Optional[Dict[str, Any]] = None,
        spark_master: Optional[str] = None,
        log_env: bool = True,
        scala_version: str = "2.12",
    ) -> None:
        super().__init__(app_name, extra_configs, spark_master, log_env)
        self.delta_version = delta_version
        self.configs = ConfigFactory(environment)
        self.clickhouse_config: BaseClickHouseConfig = self.configs.clickhouse
        self.extra_packages = extra_packages
        self.scala_version = scala_version
        self._apply_clickhouse_config()
        self.extra_configs.update({"spark.jars.packages", self._build_delta_packages()})

    def _apply_clickhouse_config(self) -> None:
        """Applies the ClickHouse configuration to the Spark session's configurations."""
        self.extra_configs.update(
            {
                "spark.sql.catalog.clickhouse": "xenon.clickhouse.ClickHouseCatalog",
                "spark.sql.catalog.clickhouse.host": self.clickhouse_config.host,
                "spark.sql.catalog.clickhouse.protocol": self.clickhouse_config.protocol,
                "spark.sql.catalog.clickhouse.http_port": self.clickhouse_config.http_port,
                "spark.sql.catalog.clickhouse.user": self.clickhouse_config.user,
                "spark.sql.catalog.clickhouse.password": self.clickhouse_config.password,
                "spark.sql.catalog.clickhouse.database": self.clickhouse_config.database,
            },
        )

    def _build_delta_packages(self) -> str:
        """Constructs the Maven artifact string for Delta Lake.

        Returns
        -------
        str
            A string representing Maven artifacts for Delta Lake.

        """
        delta_maven_artifact = f"io.delta:delta-spark_{self.scala_version}:{self.delta_version}"
        all_artifacts = [delta_maven_artifact] + (self.extra_packages if self.extra_packages else [])
        return ",".join(all_artifacts)
