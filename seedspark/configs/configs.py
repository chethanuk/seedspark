from seedspark.configs.clickhouse import (
    CIClickHouseConfig,
    LocalClickHouseConfig,
    ProdClickHouseConfig,
    StagingClickHouseConfig,
)


class ConfigFactory:
    """A factory class for generating configurations based on environment."""

    def __init__(self, env_name: str) -> None:
        """Initialize ConfigFactory with environment name.

        Parameters
        ----------
        env_name (str): Name of the environment.

        """
        self._env_name = env_name.lower()
        self._clickhouse_config = None
        self._postgres_config = None

    @property
    def clickhouse(self):
        """Lazily initialize and return ClickHouse configuration.

        Returns
        -------
        ClickHouse configuration object.

        """
        if self._clickhouse_config is None:
            self._clickhouse_config = self._get_config(
                {
                    "local": LocalClickHouseConfig,
                    "dev": LocalClickHouseConfig,
                    "ci": CIClickHouseConfig,
                    "staging": StagingClickHouseConfig,
                    "prod": ProdClickHouseConfig,
                },
            )
        return self._clickhouse_config

    def _get_config(self, config_classes):
        """Get the configuration class based on environment.

        Parameters
        ----------
        config_classes (dict): Dictionary mapping environment names to configuration classes.

        Returns
        -------
        Configuration object based on environment.

        Raises
        ------
        ValueError: If the environment name is unknown.

        """
        config_class = config_classes.get(self._env_name)
        if not config_class:
            msg = f"Unknown environment: {self._env_name}"
            raise ValueError(msg)
        return config_class()
