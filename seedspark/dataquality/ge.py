# -*- coding: utf-8 -*-
from enum import Enum
from typing import Any, Dict

from seedspark.configs import configs
from seedspark.spark import SparkAppsException
from seedspark.utils import WriteJsonToFS


class AnalyzerFormat(Enum):
    json = "json"
    df_pandas = "pandas"
    df_spark = "spark"


class GeValidationToFS(WriteJsonToFS):
    def __init__(self, file_name: str, dump_dict: Dict[Any, Any], dump_dir: str = None):
        if dump_dir is None:
            dump_dir = f'{configs["metrics_base_path"]}/{file_name}/'
        super().__init__(dump_dir, file_name, dump_dict)

    def dump(self):
        if "success" not in self.dump_dict:
            raise SparkAppsException("dump_dict is required to contain a success key", [ValueError])

        if "statistics" not in self.dump_dict:
            raise SparkAppsException("dump_dict is required to contain a statistics key", [ValueError])

        if "expectation_suite_name" not in self.dump_dict.get("meta"):
            raise SparkAppsException(
                "dump_dict is required to contain a expectation_suite_name key", [ValueError]
            )

        super().dump()
