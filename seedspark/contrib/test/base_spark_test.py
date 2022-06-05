# -*- coding: utf-8 -*-
"""Contains base Spark Test helpers and setup and teardown utils for the module tests."""

import logging
import os
import shutil
import sys
import tempfile
from pathlib import Path

from lognub import log
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


class BaseSparkTest:
    __warehouse_path = None
    __metastore_path = None
    __temp_path = None
    __debry_path = "./derby.log"
    spark_version = "3.0.2"
    spark_app_name = "spark-tests"
    spark_master = "local[4]"
    spark: SparkSession = None
    sc: SparkContext = None
    path = None

    @classmethod
    def setup_class(cls):
        # Before All
        log.info("BaseSpark - Running setup_class")
        cls.check_spark_installation()

        cls.__warehouse_path = Path("spark-warehouse", cls.random_uuid())
        cls.__metastore_path = Path("metastore_db", cls.random_uuid())
        spark_conf = (
            cls._spark_conf()
            .set("spark.sql.warehouse.dir", str(cls.__warehouse_path.absolute()))
            .set(
                "javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={str(cls.__metastore_path.absolute())};create=true",
            )
        )

        cls.spark = (
            SparkSession.builder.master(cls.spark_master)
            .appName(cls.spark_app_name)
            .config(conf=spark_conf)
            .enableHiveSupport()
            .getOrCreate()
        )

        cls.sc = cls.spark.sparkContext
        cls.quiet_py4j(cls.sc)
        log.info(f"Spark Version: {cls.spark}")
        log.info("spark and sc initialized")

    @classmethod
    def teardown_class(cls):
        # After All
        log.info("BaseSpark - Running final teardown")
        cls.clear_jvm_spark(cls.spark)
        cls.clean_tear_down_session(cls.spark)
        # cls.spark.stop()
        # if not cls.spark.sparkContext._jsc.sc().isStopped():
        #     cls.spark.stop()

    def setup_method(self):
        # Before Each
        self.__temporary_path = tempfile.TemporaryDirectory()
        self.path = Path(self.__temporary_path.name)

    def teardown_method(self):
        # After Each
        log.info("DQSpark - Running teardown_method")
        self.__temporary_path.cleanup()

        self.clean_spark(self.spark)
        self.clear_jvm_spark(self.spark)

    ############################################################################################
    ####################################### HELPERS ############################################
    ############################################################################################
    @staticmethod
    def fs_rmtree(template, _ignore_errors=True):
        """
        Recursively delete a directory tree.
        If ignore_errors is false and onerror is None, an exception is raised.
        """
        shutil.rmtree(path=Path(template), ignore_errors=_ignore_errors)

    @staticmethod
    def check_spark_installation():
        """
        check_spark_installation [summary]

        [extended_summary]
        """
        # make sure env variables are set correctly
        if "SPARK_HOME" not in os.environ:
            log.error("ERR! SPARK_HOME is not set")
            sys.exit(1)
            # os.environ["SPARK_HOME"] = "/usr/local/opt/spark"

    @staticmethod
    def _spark_conf() -> SparkConf:
        _base_spark_conf = (
            SparkConf()
            .set("spark.ui.showConsoleProgress", "false")
            .set("spark.ui.enabled", "true")
            .set("spark.sql.shuffle.partitions", "10")
            .set("spark.unsafe.exceptionOnMemoryLeak", "true")
            .set("spark.sql.broadcastTimeout", "3600")
            .set("spark.sql.session.timeZone", "UTC")
        )

        # TODO: Move to setup?
        _base_spark_conf = _base_spark_conf.set("spark.default.parallelism", "4")
        return _base_spark_conf

    @staticmethod
    def quiet_py4j(sc: SparkContext) -> None:
        """
        quiet_py4j Reuce the spark py4j log level
        """
        log4j = logging.getLogger("py4j")
        log4j.setLevel(logging.ERROR)
        logger = sc._jvm.org.apache.log4j
        logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
        logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
        logger.LogManager.getLogger("py4j").setLevel(logger.Level.ERROR)

    @classmethod
    def clean_spark_dir(cls) -> None:
        """
        clean_spark_dir Remove spark dependant directories like warehouse
        """
        try:
            os.remove(cls.__debry_path)
            shutil.rmtree(path=cls.__warehouse_path, ignore_errors=True)
            shutil.rmtree(path=cls.__metastore_path, ignore_errors=True)
        except OSError:
            print("OSError in clean_spark_dir")

    @staticmethod
    def clear_jvm_spark(session: SparkSession) -> None:
        jvm_session = session._jvm.SparkSession.getActiveSession().get()  # pylint: disable=W0212
        jvm_session.sharedState().cacheManager().clearCache()
        jvm_session.sessionState().catalog().reset()

    @staticmethod
    def clean_spark(session: SparkSession) -> None:
        """
        clean_spark Drop and Removes all cached tables from the in-memory cache

        Parameters
        ----------
        session : SparkSession
            session to clear
        """
        tables = session.catalog.listTables("default")

        for table in tables:
            print(f"clear_tables() is dropping table/view: {table.name}")
            log.info(f"clear_tables() is dropping table/view: {table.name}")
            # noinspection SqlDialectInspection,SqlNoDataSourceInspection
            session.sql(f"DROP TABLE IF EXISTS default.{table.name}")
            # noinspection SqlDialectInspection,SqlNoDataSourceInspection
            session.sql(f"DROP VIEW IF EXISTS default.{table.name}")
            # noinspection SqlDialectInspection,SqlNoDataSourceInspection
            session.sql(f"DROP VIEW IF EXISTS {table.name}")

        session.catalog.clearCache()

    @staticmethod
    def clean_tear_down_session(session: SparkSession) -> None:
        """
        clean_close Stop SparkSession safely

        Clean in-memory cache and remove spark directories and stop the session

        Parameters
        ----------
        session : SparkSession
            SparkSession which needs to be stopped
        """
        BaseSparkTest.clean_spark(session)
        BaseSparkTest.clean_spark_dir()
        session.stop()

    @staticmethod
    def random_uuid() -> str:
        import uuid

        return str(uuid.uuid4())
