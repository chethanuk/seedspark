#!/usr/bin/python
# -*- coding: utf-8 -*-
import copy
from abc import ABC, abstractmethod
from dataclasses import field
from typing import Any, Dict, List, Tuple

# Import Logger object
from lognub import log
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


class SparkApps(ABC):
    spark = None
    sc = None

    def __init__(
        self,
        app_name: str,
        extra_configs: Dict[Any, Any] = None,
        spark_master: str = None,
        create_session: bool = True,
        log_env: bool = True,
    ):
        self.extra_configs = extra_configs
        self.spark_master = spark_master
        self.app_name = app_name
        if log_env:
            self.log_sys_env()
        if create_session:
            self.sc, self.spark = self.get_or_create_spark()

    @property
    def spark_conf(self) -> SparkConf:
        # spark.driver.userClasspathFirst true
        # spark.memory.fraction 0.6
        # spark.jars.packages "io.prestosql:presto-jdbc:jar:348"
        # spark.sql.inMemorycolumnarStorage.compressed
        # spark.sql.sources.partitionColumnTypeInference.enabled
        # spark.sql.parquet.mergeSchema
        # spark.sql.extensions

        _base_spark_conf = (
            SparkConf()
            .set("spark.ui.enabled", "true")
            .set("spark.network.timeout", "1200000")
            .set("spark.rpc.numRetries", "20")
            .set("spark.task.maxFailures", "100")
            .set("spark.cleaner.periodicGC.interval", "1min")
            .set("spark.unsafe.exceptionOnMemoryLeak", "true")
            .set("spark.sql.broadcastTimeout", "3600")  # SQL
            .set("spark.dynamicAllocation.enabled", "true")
            .set("spark.sql.parquet.filterPushdown", "true")
            .set("spark.sql.parquet.recordLevelFilter.enabled", "true")
            .set("spark.sql.adaptive.enabled", "true")
            .set("spark.sql.optimizer.nestedSchemaPruning.enabled", "true")
            .set("spark.sql.optimizer.dynamicPartitionPruning", "true")
            .set("spark.sql.optimizer.dynamicPartitionPruning", "true")
            .set("spark.sql.ansi.enabled", "true")
            .set("spark.sql.cbo.enabled", "true")
            .set("spark.sql.hive.metastorePartitionPruning", "true")
            .set("spark.sql.execution.arrow.pyspark.enabled", "true")  # Arrow
            .set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
            .set("spark.sql.execution.pandas.convertToArrowArraySafely", "true")
            .set("spark.sql.execution.arrow.maxRecordsPerBatch", "50000")
            .set("spark.sql.execution.arrow.pyspark.maxRecordsPerBatch", "50000")
        )

        return _base_spark_conf

    @staticmethod
    def _py4j_logger(sc: SparkContext):
        log4j_logger = sc._jvm.org.apache.log4j  # pylint: disable=W0212
        return log4j_logger.LogManager.getLogger(__name__)

    @staticmethod
    def quiet_py4j(sc: SparkContext, level: str = "DEBUG") -> None:
        """
        quiet_py4j Reuce the spark py4j log level
        Check more at
        spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkContext.setLogLevel.html
        """
        import logging

        if level is not None and level is [
            "ALL",
            "DEBUG",
            "ERROR",
            "FATAL",
            "INFO",
            "OFF",
            "TRACE",
            "WARN",
        ]:
            sc.setLogLevel(level)
        else:
            log.info(f"Error, Wrong Py4j Log level: {level} is passed")
        log4j = logging.getLogger("py4j")
        log4j.setLevel(logging.ERROR)
        logger = sc._jvm.org.apache.log4j  # pylint: disable=W0212
        logger.LogManager.getLogger("offer").setLevel(logger.Level.DEBUG)
        logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
        logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
        logger.LogManager.getLogger("py4j").setLevel(logger.Level.ERROR)

    def get_or_create_spark(self) -> Tuple[SparkContext, SparkSession]:
        # -XX:+G1SummarizeConcMark
        extra_java_options = (
            "-XX:+UnlockDiagnosticVMOptions "
            "-XX:ParallelGCThreads=100 -XX:+UnlockExperimentalVMOptions "
            "-XX:InitiatingHeapOccupancyPercent=35 -XX:-UseGCOverheadLimit -XX:+UseG1GC"
        )
        _spark_conf = self.spark_conf
        _spark_conf = (
            _spark_conf.set("spark.python.worker.memory", "512m")
            .set("spark.driver.extraJavaOptions", extra_java_options)
            .set("spark.executor.extraJavaOptions", extra_java_options)
        )

        if self.extra_configs is not None:
            for conf in self.extra_configs:
                _spark_conf = _spark_conf.set(conf, self.extra_configs[conf])

        self.spark_conf = _spark_conf

        if self.spark_master is not None:
            spark_builder = SparkSession.builder.master(self.spark_master)
        else:
            spark_builder = SparkSession.builder

        spark: SparkSession = (
            spark_builder.appName(self.app_name).config(conf=_spark_conf).enableHiveSupport().getOrCreate()
        )

        sc: SparkContext = spark.sparkContext

        log.info(f"Spark Session Initialized: {spark} with version: {sc.version}")
        # log.info(f"""Spark SQL config:
        # {spark.sql("SET -v").select("key", "value").show(n=100, truncate=False)}""")
        return sc, spark

    @spark_conf.setter
    def spark_conf(self, value):
        self._spark_conf = value

    def stop_spark_session(self):
        log.info("Stopping spark application")
        self._sleep()
        self.spark.stop()
        log.info("Spark application is stopped and sys.exit(0)")
        import sys

        sys.exit(0)

    @staticmethod
    def _sleep(seconds=5):
        import time

        time.sleep(seconds)

    @staticmethod
    def log_sys_env():
        import subprocess

        java_version = subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT)
        log.info(f"Java Version: {java_version}")
        import pkg_resources

        installed_packages_list = sorted([f"{i.key}=={i.version}" for i in pkg_resources.working_set])
        log.info(f"installed_packages_list: {installed_packages_list}")

    @abstractmethod
    def execute(self):
        raise SparkAppsException("No Implementation has found for execute()", NotImplementedError)


class SparkAppsException(Exception):
    """
    SparkAppsException Common base class for all non-exit EasySpark exceptions

    Parameters
    ----------
    message : str
        [description]
    exceptions : Optional[List[Exception]], optional
        [description], by default None

    Usage
    ----------
    > raise SparkAppsException(message="DF len is less than 1")
    > raise SparkAppsException(message="DF len is less than 1", exceptions=[KeyError])
    """

    def __init__(
        self, message: str, exception: Exception = None, exceptions_list: List[Exception] = None
    ) -> None:
        self.message: str = message
        self.exception: Exception = exception
        # self.exceptions: Optional[List[Exception]] = exceptions if exceptions else []
        _expectations: List[Exception] = exceptions_list if exceptions_list else []
        self.exceptions: List[Exception] = _expectations + [self.exception]

        if self.exceptions is not None:
            super().__init__(
                f"{self.message}, Total {len(self.exceptions)}. "
                f"Failures: {', '.join(str(e) for e in self.exceptions)}"
            )

        else:
            super().__init__(f"SparkAppsException: {self.message}")

    @staticmethod
    def set_default_field(obj):
        return field(default_factory=lambda: copy.copy(obj))
