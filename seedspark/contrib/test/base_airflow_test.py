# -*- coding: utf-8 -*-
"""Contains base Spark Test helpers and setup and teardown utils for the module tests."""


# Import Logger object

# def mock_dag_args()


class BaseAirflowTest:
    __warehouse_path = None
    __metastore_path = None
    __temp_path = None
    path = None
    dag_objects = None
    #
    # @pytest.fixture(autouse=True)
    # def mock_utils(self, myfixture):
    #     pass
    #
    # @classmethod
    # def setup_class(cls):
    #     # Before All
    #     log.info(f"BaseSpark - Running setup_class")
    #     # cls.
    #
    #     log.info(f"spark and sc initialized")
    #
    # @classmethod
    # def teardown_class(cls):
    #     # After All
    #     log.info(f"BaseSpark - Running final teardown")
    #     # cls.spark.stop()
    #     # if not cls.spark.sparkContext._jsc.sc().isStopped():
    #     #     cls.spark.stop()
    #
    # def setup_method(self):
    #     # Before Each
    #     self.__temporary_path = tempfile.TemporaryDirectory()
    #     self.path = Path(self.__temporary_path.name)
    #     self.dag_objects = self._get_dag_objects(self.dag_files, self.dags_path)
    #
    # def teardown_method(self):
    #     # After Each
    #     log.info(f"DQSpark - Running teardown_method")
    #     self.__temporary_path.cleanup()
    #
    #     # self.clean_spark(self.spark)
    #     # self.clear_jvm_spark(self.spark)
    #
    # @property
    # @abstractmethod
    # def dags_path(self):
    #     """
    #     Path of DAG file or folder
    #     Example Value:
    #         `os.path.join(os.path.dirname(__file__), "..", "..", "dags/**/*.py")`,
    #         `../dags/**/*.py`
    #     """
    #     raise NotImplementedError("Define dags_path")
    #
    # @property
    # def dag_files(self):
    #     """
    #     List of all airflow dag files
    #     Raises the Value error when dag path is not defined properly
    #     """
    #     dag_files = None
    #     if self.dags_path is not None:
    #         dag_files = glob.glob(self.dags_path, recursive=True)
    #     else:
    #         value_err_msg = f"dag_path property {self.dags_path} is not defined properly"
    #         log.error(value_err_msg)
    #         # raise ValueError(value_err_msg)
    #     return dag_files
    #
    # ############################################################################################
    # ####################################### HELPERS ############################################
    # ############################################################################################
    # @staticmethod
    # def fs_rmtree(template, _ignore_errors=True):
    #     """
    #     Recursively delete a directory tree.
    #     If ignore_errors is false and onerror is None, an exception is raised.
    #     """
    #     shutil.rmtree(path=Path(template), ignore_errors=_ignore_errors)
    #
    # @staticmethod
    # def _get_dag_objects(dag_file, dag_path):
    #     module_name, _ = os.path.splitext(dag_file)
    #     module_path = os.path.join(dag_path, dag_file)
    #     mod_spec = importlib.util.spec_from_file_location(module_name, module_path)
    #     module = importlib.util.module_from_spec(mod_spec)
    #     mod_spec.loader.exec_module(module)
    #     dag_objects = [var for var in vars(module).values() if isinstance(var, DAG)]
    #     return dag_objects
