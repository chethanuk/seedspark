# -*- coding: utf-8 -*-
import json
import os
from typing import Any, Dict

from lognub import log

from seedspark.spark import SparkAppsException


def make_sure_path_exists(path):
    """Ensure that a directory exists.
    :param path: A directory path.
    """

    # os.makedirs(path, exist_ok=True)
    log.debug(f"Making sure path exists: {path}")
    try:
        os.makedirs(path, exist_ok=True)
        log.debug("Created directory at: %s", path)
    except OSError as exception:
        log.error(f"make_sure_path_exists - Exception: {exception}")
        return False
    return True


class WriteJsonToFS:
    def __init__(self, dump_dir: str, file_name: str, dump_dict: Dict[Any, Any]):
        self.dump_dir = dump_dir
        self.file_name = file_name
        self.dump_dict = dump_dict

    def dump(self):
        if not make_sure_path_exists(self.dump_dir):
            raise SparkAppsException(f"Unable to create dir at {self.dump_dir}", [IOError])

        if not isinstance(self.file_name, str):
            raise SparkAppsException("Template name is required to be of type str", [TypeError])

        if not isinstance(self.dump_dict, dict):
            raise SparkAppsException("dump_dict is required to be of type dict", [TypeError])

        json_file = self.get_file_name(self.dump_dir, self.file_name)

        with open(json_file, "w") as outfile:
            json.dump(self.dump_dict, outfile, indent=2)
        return json_file

    @staticmethod
    def get_file_name(dump_dir, file_name):
        """Get the name of file."""
        suffix = ".json" if not file_name.endswith(".json") else ""
        file_name = f"{file_name}.{suffix}"
        return os.path.join(dump_dir, file_name)
