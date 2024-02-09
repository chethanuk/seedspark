#!/usr/bin/python
import subprocess
from abc import ABC, abstractmethod
from typing import Any, Dict

import pkg_resources
from loguru import logger as log
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


class SparkAppsException(Exception):
    """Custom Exception for SparkApps."""


class SparkApps(ABC):
    """Base class for creating Spark applications with optimized configurations and functionalities.

    Parameters
    ----------
    app_name : str
        Name of the Spark application.
    extra_configs : dict, optional
        Additional Spark configurations in key-value pairs.
    spark_master : str, optional
        Master URL for the Spark session.
    log_env : bool, default True
        Flag to enable logging of the system environment.

    Attributes
    ----------
    _spark : SparkSession, private
        Lazily initialized Spark session.
    _sc : SparkContext, private
        Lazily initialized Spark context.

    """

    def __init__(
        self,
        app_name: str,
        extra_configs: Dict[str, Any] = None,
        spark_master: str = None,
        log_env: bool = True,
    ):
        self.app_name = app_name
        self.extra_configs = extra_configs or {}
        self.spark_master = spark_master
        self._spark = None
        self._sc = None
        if log_env:
            self.log_sys_env()

    @property
    def spark_conf(self) -> SparkConf:
        """Configures and returns a SparkConf object with basic and adaptive query execution settings.

        Returns
        -------
        SparkConf
            Configured SparkConf object.

        """
        spark_conf = SparkConf().setAppName(self.app_name)
        if self.spark_master:
            spark_conf.setMaster(self.spark_master)
        for key, value in self.extra_configs.items():
            spark_conf.set(key, value)
        self._set_adaptive_configs(spark_conf)
        return spark_conf

    @property
    def spark(self) -> SparkSession:
        """Lazily initializes and returns the SparkSession object.

        Returns
        -------
        SparkSession
            Initialized SparkSession object.

        """
        if not self._spark:
            self._initialize_spark_session()
        return self._spark

    @property
    def sc(self) -> SparkContext:
        """Lazily initializes and returns the SparkContext object.

        Returns
        -------
        SparkContext
            Initialized SparkContext object.

        """
        if not self._sc:
            self._initialize_spark_session()
        return self._sc

    def _initialize_spark_session(self):
        """Initializes the SparkSession and SparkContext."""
        spark_conf = self.spark_conf
        self._spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
        self._sc = self._spark.sparkContext
        log.info(f"Spark Session Initialized: {self._spark} with version: {self._sc.version}")

    def _set_adaptive_configs(self, spark_conf: SparkConf):
        """Sets adaptive query execution configurations if enabled."""
        if self.extra_configs.get("enableAdaptiveQueryExecution", False):
            spark_conf.set("spark.sql.adaptive.enabled", "true")
            spark_conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            spark_conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

    def stop_spark_session(self):
        """Stops the Spark session if initialized."""
        if self._spark:
            log.info("Stopping spark application")
            self._spark.stop()
            log.info("Spark application stopped")
        else:
            log.info("Spark session not initialized")

    @staticmethod
    def log_sys_env():
        """Logs the Java version and installed Python packages."""
        try:
            java_version = subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT).decode()
            log.info(f"Java Version: {java_version}")
        except subprocess.CalledProcessError as e:
            log.error(f"Error obtaining Java version: {e}")
        installed_packages_list = sorted([f"{i.key}=={i.version}" for i in pkg_resources.working_set])
        log.info(f"Installed packages: {installed_packages_list}")

    @abstractmethod
    def execute(self):
        """Abstract method to be implemented by subclasses.

        Raises
        ------
        NotImplementedError
            If the subclass does not implement this method.

        """
        raise NotImplementedError("execute method must be implemented")
