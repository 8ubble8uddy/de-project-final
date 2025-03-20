import logging

import pendulum
from airflow.decorators import dag
from airflow.providers.vertica.operators.vertica import VerticaOperator

from py.app_config import AppConfig


config = AppConfig()

logger = logging.getLogger(__name__)

default_args = {
    'start_date': pendulum.datetime(2022, 10, 1),
    'end_date': pendulum.datetime(2022, 11, 1),
}

@dag('2_datamart_update', schedule='@daily', template_searchpath='/lessons/sql/cdm', default_args=default_args, max_active_runs=1)
def datamart_update():

    update_global_metrics = VerticaOperator(
        task_id='MERGE_global_metrics',
        vertica_conn_id=config.VERTICA_WAREHOUSE_CONN_ID,
        sql='/MERGE_global_metrics.sql',
    )


_ = datamart_update()
