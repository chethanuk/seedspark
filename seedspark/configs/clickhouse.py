import os
from dataclasses import dataclass


@dataclass
class BaseClickHouseConfig:
    """Base configuration for ClickHouse with default settings.
    Attributes are set using properties with fallbacks to default values.

    Attributes
    ----------
    HOST : str
        The host address of the ClickHouse server.
    PROTOCOL : str
        The protocol used to connect to ClickHouse (e.g., http).
    HTTP_PORT : str
        The port for HTTP connections to ClickHouse.
    USER : str
        Username for ClickHouse authentication.
    PASSWORD : str
        Password for ClickHouse authentication.
    DATABASE : str
        The default database to use in ClickHouse.

    """

    def get_conf(self, var_name: str, default: str) -> str:
        return os.getenv(var_name, default)

    @property
    def host(self) -> str:
        #
        return self.get_conf("CLICKHOUSE_HOST", "github.demo.altinity.cloud")

    @property
    def protocol(self) -> str:
        return self.get_conf("CLICKHOUSE_PROTOCOL", "https")

    @property
    def http_port(self) -> int:
        return int(self.get_conf("CLICKHOUSE_JDBC_PORT", "8443"))

    @property
    def user(self) -> str:
        return self.get_conf("CLICKHOUSE_USER", "demo")

    @property
    def password(self) -> str:
        return self.get_conf("CLICKHOUSE_PASSWORD", "demo")

    @property
    def database(self) -> str:
        return self.get_conf("CLICKHOUSE_DATABASE", "default")

    @property
    def ssl(self) -> bool:
        return True


class LocalClickHouseConfig(BaseClickHouseConfig):
    """Local environment configuration for ClickHouse."""


class CIClickHouseConfig(BaseClickHouseConfig):
    """CI environment configuration for ClickHouse."""


class StagingClickHouseConfig(BaseClickHouseConfig):
    """Staging environment configuration for ClickHouse."""


class ProdClickHouseConfig(BaseClickHouseConfig):
    """Production configuration for ClickHouse, overrides from environment variables."""

    @property
    def ssl(self) -> bool:
        return True
