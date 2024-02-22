from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task

log = logging.getLogger(__name__)


def task_failure_alert(context):
    # TODO: Impliment Slack alert
    print(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")
    # Send Slack
    log.info(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")


# When using the DAG decorator, The "dag_id" value defaults to the name of the function
# it is decorating if not explicitly set. In this example, the "dag_id" value would be "example_dag_basic".
@dag(
    # This defines how often your DAG will run, or the schedule by which your DAG runs. In this case, this DAG
    # will run daily
    schedule="@daily",
    # This DAG is set to run for the first time on January 1, 2023. Best practice is to use a static
    # start_date. Subsequent DAG runs are instantiated based on the schedule
    start_date=datetime(2024, 2, 9),
    # When catchup=False, your DAG will only run the latest run that would have been scheduled. In this case, this means
    # that tasks will not be run between January 1, 2023 and 30 mins ago. When turned on, this DAG's first
    # run will be for the next 30 mins, per the its schedule
    catchup=False,
    default_args={
        "retries": 2,  # If a task fails, it will retry 2 times.
        "owner": "chethanuk",
    },
    tags=["example"],
    # When failure - trigger alert
    on_failure_callback=task_failure_alert,
)  # If set, this tag is shown in the DAG view of the Airflow UI
def clickhouse_simple_elt():
    """
    ### TaskFlow API example using virtualenv
    This is a simple data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    """

    @task()
    def extract_load_sqlite():
        from utils.airports_elt import ClickhouseToSQLitePipeline

        pipeline = ClickhouseToSQLitePipeline()
        return pipeline.run()

    extract_load_sqlite()


clickhouse_simple_elt_dag = clickhouse_simple_elt()
