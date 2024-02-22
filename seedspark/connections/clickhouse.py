from __future__ import annotations

import warnings

# from typing import ClassVar
from onetl._util.classproperty import classproperty
from onetl.connection.db_connection.jdbc_connection import JDBCConnection
from onetl.connection.db_connection.jdbc_mixin.options import JDBCOptions
from onetl.hooks import slot, support_hooks
from onetl.impl import GenericOptions

from seedspark.connections.clickhouse_dialect import ClickhouseDialect


class ClickHouseExtra(GenericOptions):
    class Config:
        extra = "allow"
        ssl = "true"


@support_hooks
class ClickHouse(JDBCConnection):
    """ClickHouse JDBC connection. |support_hooks|

    Based on Maven package ``com.clickhouse:clickhouse-jdbc:0.6.0``
    (official ClickHouse JDBC driver).

    ... [Additional documentation and version compatibility details]

    Parameters
    ----------
    host : str
        Host of ClickHouse database.

    port : int, default: ``8123``
        HTTP port of ClickHouse database.

    user : str
        User with access to the database.

    password : str
        Password for database connection.

    database : str
        Target database in ClickHouse.

    spark : :obj:`pyspark.sql.SparkSession`
        Spark session.

    extra : dict, default: ``None``
        Additional connection parameters.

    ... [Additional parameter documentation]

    Examples
    --------
    ... [Usage examples]
    """

    database: str
    port: int = 8123  # Default port for ClickHouse
    extra: ClickHouseExtra = ClickHouseExtra()

    Extra = ClickHouseExtra
    Dialect = ClickhouseDialect
    DRIVER: str = "com.clickhouse.jdbc.ClickHouseDriver"

    @slot
    @classmethod
    def get_packages(cls) -> list[str]:
        """Get Maven packages for Spark."""
        return ["com.clickhouse:clickhouse-jdbc:0.6.0"]

    @classproperty
    def package(cls) -> str:
        """Get package name to be downloaded by Spark."""
        msg = "`Clickhouse.package` will be removed in 1.0.0, use `Clickhouse.get_packages()` instead"
        warnings.warn(msg, UserWarning, stacklevel=3)
        return "com.clickhouse:clickhouse-jdbc:0.6.0"

    @property
    def jdbc_url(self) -> str:
        extra = self.extra.dict(by_alias=True)
        parameters = "&".join(f"{k}={v}" for k, v in sorted(extra.items()))
        return f"jdbc:clickhouse://{self.host}:{self.port}/{self.database}?{parameters}".rstrip("?")

    @property
    def instance_url(self) -> str:
        return f"{super().instance_url}/{self.database}"

    def _options_to_connection_properties(self, options: JDBCOptions):
        return super()._options_to_connection_properties(options)
