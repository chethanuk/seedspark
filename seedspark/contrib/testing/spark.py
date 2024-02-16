import logging
import tempfile
import uuid
from pathlib import Path

import pytest
from pyspark import SparkConf
from pyspark.sql import SparkSession

logger = logging.getLogger("spark-test-logger")


class BaseSparkTest:
    """Base class for Spark tests with optimized Spark session initialization and teardown."""

    _spark = None

    @property
    def spark(self):
        """Lazily initialized Spark session. Creates a Spark session when first accessed."""
        if self._spark is None:
            logger.info("Initializing Spark session for testing")
            spark_conf = self.create_spark_conf()
            self._spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
            self.quiet_py4j(self._spark.sparkContext)
        return self._spark

    @classmethod
    def teardown_class(cls) -> None:
        """Class-level teardown to stop the Spark session."""
        if cls._spark:
            cls._spark.stop()
            cls._spark = None
            logger.info("Spark session for tests stopped")

    @staticmethod
    def create_spark_conf() -> SparkConf:
        """Creates a SparkConf object with settings optimized for testing."""
        return (
            SparkConf()
            .setAppName("spark-tests")
            .setMaster("local[2]")
            .set("spark.ui.showConsoleProgress", "false")
            .set("spark.sql.shuffle.partitions", "4")
        )

    @staticmethod
    def quiet_py4j(sc) -> None:
        """Reduces the log level of Spark's py4j to minimize noise during testing."""
        logging.getLogger("py4j").setLevel(logging.ERROR)
        sc.setLogLevel("ERROR")

    @pytest.fixture()
    def sparkSession(self):
        """Function-level fixture to manage temporary directories for each test."""
        temp_dir = tempfile.TemporaryDirectory()
        self.test_path = Path(temp_dir.name)
        yield self.spark
        temp_dir.cleanup()

    @staticmethod
    def random_uuid() -> str:
        """Generates a random UUID."""
        return str(uuid.uuid4())
