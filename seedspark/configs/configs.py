#!/usr/bin/python
# -*- coding: utf-8 -*-
import getpass
import os
from pathlib import Path

from lognub import log

data_folder_path: str = Path(__file__).parent.parent.parent.absolute().__str__()

# class Configuration: https://pypi.org/project/injector/
configs = {
    "env": f"{os.getenv('env', 'development')}",
    "timeoutSeconds": 30,
}

whoami = getpass.getuser()
if whoami == "chethanuk" and configs["env"] == "None":
    log.info(f"Set env as 'development' since the user is {whoami}")
    configs["env"] = "development"

env = configs["env"]
if env == "production":
    configs["gcs_base_path"] = f"gs://{env}-data/v1/"
elif env == "development":
    configs["gcs_datasets_path"] = f"{data_folder_path}/data"
    configs["gcs_base_path"] = f'{configs["gcs_datasets_path"]}/dev'
    configs["dataquality_base_path"] = f"{data_folder_path}/sparkapps/dataquality"
    configs["example"] = f"{data_folder_path}/data/example"
    configs["metrics_base_path"] = f'{configs["gcs_base_path"]}/metrics'
    configs["expectations_path"] = f'{configs["dataquality_base_path"]}/expectations'
else:
    env = "staging"
    configs["gcs_base_path"] = f"gs://{env}-data/v1/"
