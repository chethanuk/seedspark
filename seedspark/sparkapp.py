import importlib.metadata
import os
import subprocess
import urllib
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import requests
from loguru import logger as log
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from seedspark.decorators import log_exceptions

# Constants
HTTP_STATUS_OK = 200
REQUEST_TIMEOUT = 10  # in seconds
HTTP_SCHEMES = ["http", "https"]
JAR_DIR_DEFAULT = "jars"
DELTA_MAVEN_FORMAT = "io.delta:delta-spark_{scala_version}:{delta_version}"


class SparkAppsException(Exception):
    """Custom Exception for SparkApps."""


class SparkApps(ABC):
    """Base class for Spark applications with optimized configurations and functionalities."""

    def __init__(  # noqa: PLR0913
        self,
        app_name: str,
        extra_packages: Optional[List[str]] = None,
        extra_jars: Optional[List[str]] = None,
        extra_configs: Optional[Dict[str, Any]] = None,
        spark_master: Optional[str] = None,
        environment="staging",
        log_env: bool = True,
        scala_version: str = "2.12",
        enable_delta_jar: bool = True,
        delta_version: str = "3.1.0",
        jar_dir: str = JAR_DIR_DEFAULT,
    ) -> None:
        self.app_name = app_name
        if enable_delta_jar:
            self.extra_configs = {**(self._apply_delta_config()), **(extra_configs or {})}
        self.spark_master = spark_master
        self._sc = None
        self.jar_dir = jar_dir
        self.delta_version = delta_version
        self.scala_version = scala_version
        self.enable_delta_jar = enable_delta_jar
        self.environment = environment
        self.extra_jars = self.resolve_jar_urls(extra_jars)
        self.extra_packages = extra_packages or []
        if log_env:
            self.log_sys_env()
        self._spark = self._initialize_spark_session()

    @property
    @log_exceptions
    def spark_conf(self) -> SparkConf:
        """Property to get SparkConf object with all configurations set."""
        spark_conf = SparkSession.builder.appName(self.app_name)
        if self.spark_master:
            spark_conf.master(self.spark_master)
        if self.extra_packages or self.enable_delta_jar:
            packages = self._build_maven_packages()
            spark_conf.config("spark.jars.packages", packages)
            log.info(f"Setting spark.jars.packages to {packages}")
        if self.extra_jars:
            spark_conf.config("spark.jars", ",".join(self.resolve_jar_urls(self.extra_jars)))
        for key, value in self.extra_configs.items():
            spark_conf.config(key, value)
        self._set_adaptive_configs(spark_conf)
        return spark_conf

    @abstractmethod
    def execute(self):
        """Abstract method to be implemented by subclasses."""
        raise NotImplementedError("execute method must be implemented")

    @property
    @log_exceptions
    def spark(self) -> SparkSession:
        """Lazily initializes and returns the SparkSession object."""
        return self._spark

    @property
    @log_exceptions
    def sc(self) -> SparkContext:
        """Lazily initializes and returns the SparkContext object."""
        return self.spark.sparkContext

    @log_exceptions
    def _initialize_spark_session(self) -> None:
        """Initializes the SparkSession and SparkContext."""
        _spark_conf = self.spark_conf
        log.info("Initializing SparkSession")
        _spark = _spark_conf.getOrCreate()
        log.info(
            f"SparkSession initialized with version: {_spark.version} and configurations: {_spark.sparkContext.getConf().getAll()}"
        )
        return _spark

    @log_exceptions
    def _set_adaptive_configs(self, spark_conf: SparkConf) -> None:
        """Sets adaptive query execution configurations if enabled."""
        if self.extra_configs.get("enableAdaptiveQueryExecution", False):
            spark_conf.config("spark.sql.adaptive.enabled", "true")
            spark_conf.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            spark_conf.config("spark.sql.adaptive.skewJoin.enabled", "true")

    @log_exceptions
    def stop_spark_session(self) -> None:
        """Stops the Spark session if initialized."""
        if self._spark:
            self._spark.stop()

    @staticmethod
    @log_exceptions
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

    @log_exceptions
    def resolve_jar_urls(self, jar_urls) -> List[str]:
        """Resolves JAR URLs, downloading if necessary."""
        if not isinstance(jar_urls, list) or any(not isinstance(url, str) for url in jar_urls):
            raise TypeError("jar_urls must be a list of strings.")

        resolved_jars = []
        for jar in jar_urls:
            if jar.startswith("http://") or jar.startswith("https://"):
                resolved_jars.append(self._download_jar(jar))
            else:
                resolved_jars.append(jar)
        return resolved_jars

    @log_exceptions
    def _download_jar(self, url: str) -> str:
        """Downloads a JAR file from a URL."""
        parsed_url = urllib.parse.urlparse(url)
        if parsed_url.scheme not in HTTP_SCHEMES:
            raise ValueError("Invalid URL scheme. Only HTTP and HTTPS are allowed.")
        os.makedirs(self.jar_dir, exist_ok=True)
        file_name = os.path.join(self.jar_dir, os.path.basename(parsed_url.path))
        try:
            response = requests.get(url, stream=True, timeout=REQUEST_TIMEOUT)
            if response.status_code == HTTP_STATUS_OK:
                with open(file_name, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                return file_name
            else:
                raise SparkAppsException(f"Failed to download file, HTTP status code: {response.status_code}")
        except Exception as e:
            log.error(f"Failed to download JAR from {url}: {e}")
            raise

    @log_exceptions
    def _build_maven_packages(self) -> str:
        """Constructs the Maven artifact string for Delta Lake."""
        delta_maven_artifact = DELTA_MAVEN_FORMAT.format(
            scala_version=self.scala_version, delta_version=self.delta_version
        )
        all_artifacts = [delta_maven_artifact, *self.extra_packages] if self.enable_delta_jar else self.extra_packages
        return ",".join(all_artifacts)
