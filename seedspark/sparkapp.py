#!/usr/bin/python
import importlib.metadata
import logging
import os
import subprocess
import urllib
from abc import ABC, abstractmethod
from typing import Any, Dict, List, NoReturn, Optional

import requests
from loguru import logger as log
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# Define a constant for the HTTP status code
HTTP_STATUS_OK = 200
REQUEST_TIMEOUT = 10  # Timeout in seconds for HTTP requests


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

    def __init__(  # noqa: PLR0913
        self,
        app_name: str,
        extra_packages: Optional[List[str]] = None,
        extra_jars: Optional[List[str]] = None,
        extra_configs: Optional[Dict[str, Any]] = None,
        spark_master: Optional[str] = None,
        environment="staging",
        log_env: bool = True,
        scala_version: str = "2.13",
        enable_delta_jar: bool = True,
        delta_version: str = "3.1.0",
        jar_dir: str = "jars",  # Default directory for JAR download files
    ) -> None:
        self.app_name = app_name
        self.extra_configs = extra_configs or {}
        self.spark_master = spark_master
        self._spark = None
        self._sc = None
        self.jar_dir = jar_dir  # Store the JAR directory
        self.delta_version = delta_version
        self.scala_version = scala_version
        self.enable_delta_jar = enable_delta_jar
        self.environment = environment

        # Handling extra jars and packages
        self.handle_extra_jars(extra_jars)
        self.extra_packages = extra_packages or []

        if log_env:
            self.log_sys_env()

    @property
    def spark_conf(self) -> SparkConf:
        spark_conf = SparkConf().setAppName(self.app_name)
        if self.spark_master:
            spark_conf.setMaster(self.spark_master)

        # Set the spark.jars.packages configuration
        if self.extra_packages:
            spark_conf.set("spark.jars.packages", ",".join(self.extra_packages))

        # Set the spark.jars configuration
        if self.extra_jars:
            resolved_jars = []
            for jar in self.extra_jars:
                if jar.startswith("http://") or jar.startswith("https://"):
                    resolved_jars.append(self._download_jar(jar))
                else:
                    resolved_jars.append(jar)
            spark_conf.set("spark.jars", ",".join(resolved_jars))

        for key, value in self.extra_configs.items():
            spark_conf.set(key, value)

        self._set_adaptive_configs(spark_conf)
        return spark_conf

    @abstractmethod
    def execute(self) -> NoReturn:
        """Abstract method to be implemented by subclasses.

        Raises
        ------
        NotImplementedError
            If the subclass does not implement this method.

        """
        msg = "execute method must be implemented"
        raise NotImplementedError(msg)

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
        return self.spark.sparkContext

    def _initialize_spark_session(self) -> None:
        """Initializes the SparkSession and SparkContext."""
        spark_conf = self.spark_conf
        self._spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
        self._sc = self._spark.sparkContext
        log.info(f"Spark Session Initialized: {self._spark} with version: {self._sc.version}")

    def _set_adaptive_configs(self, spark_conf: SparkConf) -> None:
        """Sets adaptive query execution configurations if enabled."""
        if self.extra_configs.get("enableAdaptiveQueryExecution", False):
            spark_conf.set("spark.sql.adaptive.enabled", "true")
            spark_conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            spark_conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

    def stop_spark_session(self) -> None:
        """Stops the Spark session if initialized."""
        if self._spark:
            log.info("Stopping spark application")
            self._spark.stop()
            log.info("Spark application stopped")
        else:
            log.info("Spark session not initialized")

    @staticmethod
    def log_sys_env() -> None:
        """Logs the Java version and installed Python packages."""
        try:
            java_version = subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT).decode()
            log.info(f"Java Version: {java_version}")
        except subprocess.CalledProcessError as e:
            log.error(f"Error obtaining Java version: {e}")
        installed_packages_list = sorted(
            f"{dist.metadata['Name']}=={dist.version}" for dist in importlib.metadata.distributions()
        )
        log.info(f"Installed packages: {installed_packages_list}")

    def handle_extra_jars(self, extra_jars: Optional[List[str]]) -> None:
        """Handles extra JAR dependencies."""
        self.extra_jars = []
        if extra_jars:
            for jar in extra_jars:
                if jar.startswith("http://") or jar.startswith("https://"):
                    self.extra_jars.append(self._download_jar(jar))
                else:
                    self.extra_jars.append(jar)
            log.info(f"Resolved extra jars: {self.extra_jars}")
        else:
            log.info("No extra jars provided")

    def _download_jar(self, url: str) -> str:
        """
        Downloads a JAR file from a URL to the specified directory, ensuring that the URL is valid and uses a safe scheme.

        Parameters
        ----------
        url : str
            The URL of the JAR file to download.

        Returns
        -------
        str
            The file path of the downloaded JAR file.
        """
        try:
            # Validate the URL
            parsed_url = urllib.parse.urlparse(url)
            if parsed_url.scheme not in ["http", "https"]:
                raise ValueError("Invalid URL scheme. Only HTTP and HTTPS are allowed.")

            os.makedirs(self.jar_dir, exist_ok=True)  # Create the directory if it doesn't exist
            file_name = os.path.join(self.jar_dir, os.path.basename(parsed_url.path))  # Construct the file path

            # Use requests to download the file with a timeout
            response = requests.get(url, stream=True, timeout=REQUEST_TIMEOUT)
            if response.status_code == HTTP_STATUS_OK:
                with open(file_name, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                log.info(f"Downloaded Jar to {file_name} from url {url}")
                return file_name
            else:
                raise Exception(f"Failed to download file, HTTP status code: {response.status_code}")

        except Exception as e:
            logging.error(f"Failed to download JAR from {url}: {e}")
            raise

    def _build_maven_packages(self) -> str:
        """Constructs the Maven artifact string for Delta Lake.

        Returns
        -------
        str
            A string representing Maven artifacts for Delta Lake.
        """
        if self.enable_delta_jar:
            delta_maven_artifact = f"io.delta:delta-spark_{self.scala_version}:{self.delta_version}"
            all_artifacts = [delta_maven_artifact] + (self.extra_packages if self.extra_packages else [])
        else:
            all_artifacts = self.extra_packages if self.extra_packages else []
        return ",".join(all_artifacts)
