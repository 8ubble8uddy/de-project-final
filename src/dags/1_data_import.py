import glob
import logging
import csv
import os
import re
from pathlib import Path
from io import TextIOWrapper
from tempfile import NamedTemporaryFile


import pendulum
import pandas as pd
from pendulum import DateTime
from airflow.decorators import dag, task, task_group
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.filesystem import FSHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.providers.vertica.operators.vertica import VerticaOperator

from py.app_config import AppConfig


config = AppConfig()

logger = logging.getLogger(__name__)

default_args = {
    'start_date': pendulum.datetime(2022, 10, 1),
    'end_date': pendulum.datetime(2022, 11, 1),
}


@dag('1_data_import', schedule='@daily', template_searchpath='/', max_active_runs=1, default_args=default_args)
def data_import():

    @task_group(group_id='EXTRACT')
    def extract(s3_conn_id: str, fs_conn_id: str):

        s3_sensor = S3KeySensor(
            task_id='s3_sensor',
            aws_conn_id=s3_conn_id,
            bucket_name=config.S3_BUCKET_FINAL_PROJECT,
            bucket_key=[config.S3_KEY_TRANSACTIONS_BATCH, config.S3_KEY_CURRENCIES_HISTORY],
            wildcard_match=True,
            poke_interval=10,
            timeout=10,
            retries=2
        )


        @task
        def get_state(s3_key: str, ti: TaskInstance = None):
            current_object = ti.xcom_pull(key=s3_key, include_prior_dates=True)
            return current_object or s3_key


        @task
        def list_keys(s3_key: str):
            s3_hook = S3Hook(s3_conn_id)
            prefix = re.split(r"[\[\*\?]", s3_key, 1)[0]
            metadata = s3_hook.get_file_metadata(prefix)
            keys = sorted([d['Key'] for d in metadata], key=len)
            offset = keys.index(s3_key) if s3_key in keys else 0
            return keys[offset:]


        @task
        def get_increment(s3_keys: list[str], date_col: str, data_interval_end: DateTime = None):
            s3_hook = S3Hook(s3_conn_id)
            res = []
            for k in s3_keys:
                response = s3_hook.get_key(k).get(Range='bytes=0-1024')
                content = TextIOWrapper(response['Body'], 'utf-8')
                line = next(csv.DictReader(content), None)

                if line and pendulum.parse(line[date_col]) <= data_interval_end:
                    res.append(k)
                else:
                    break

            return res
        

        @task
        def save_state(s3_key: str, current_objects: list, ti: TaskInstance = None):
            if current_objects:
                ti.xcom_push(key=s3_key, value=list(current_objects)[-1])


        @task
        def download_file(s3_key: str, ds_nodash: str = None):
            s3_hook, fs_hook = S3Hook(s3_conn_id), FSHook(fs_conn_id)
            file_path = s3_hook.download_file(s3_key)
            dest_path = os.path.join(fs_hook.basepath, f'{ds_nodash}_RAW_{s3_key}')
            os.replace(file_path, dest_path)
            return dest_path


        get_last = get_state(config.S3_KEY_TRANSACTIONS_BATCH)
        keys_list = list_keys(get_last)
        keys_inc = get_increment(keys_list, date_col='transaction_dt')
        save_last = save_state(config.S3_KEY_TRANSACTIONS_BATCH, keys_inc)
        download_transactions_bathes = download_file.expand(s3_key=keys_inc)
        download_currencies_history = download_file(config.S3_KEY_CURRENCIES_HISTORY)

        s3_sensor >> get_last >> keys_list >> keys_inc >> save_last >> [download_transactions_bathes, download_currencies_history]


    @task_group(group_id='TRANSFORM')
    def transform(fs_conn_id: str):

        file_sensor = (
            FileSensor
            .partial(task_id='file_sensor', fs_conn_id=fs_conn_id)
            .expand(filepath=[
                '{{ ds_nodash }}_RAW_' + f for f in (config.S3_KEY_TRANSACTIONS_BATCH, config.S3_KEY_CURRENCIES_HISTORY)
            ])
        )

        @task
        def transform_transactions_batches(src_file: str, dst_file: str, data_interval_start: DateTime = None, data_interval_end: DateTime = None, ds_nodash: str = None):
            fs_hook = FSHook(fs_conn_id)
            src_path = os.path.join(fs_hook.basepath, f'{ds_nodash}_RAW_{src_file}')
            dst_path = os.path.join(fs_hook.basepath, f'{ds_nodash}_ODS_{dst_file}')

            dfs_list = [pd.read_csv(f) for f in glob.glob(src_path)]
            df = pd.concat(dfs_list, ignore_index=True)
            df.query('@data_interval_start <= @pd.to_datetime(transaction_dt, utc=True) < @data_interval_end', inplace=True)
            df.query('account_number_from > 0 & account_number_to > 0', inplace=True)
            df.to_csv(dst_path, index=False)
            return dst_path

        @task
        def transform_currencies_history(src_file: str, dst_file: str, data_interval_start: DateTime = None, data_interval_end: DateTime = None, ds_nodash: str = None):
            fs_hook = FSHook(fs_conn_id)
            src_path = os.path.join(fs_hook.basepath, f'{ds_nodash}_RAW_{src_file}')
            dst_path = os.path.join(fs_hook.basepath, f'{ds_nodash}_ODS_{dst_file}')

            df = pd.read_csv(src_path)
            df.query('@data_interval_start <= @pd.to_datetime(date_update, utc=True) < @data_interval_end', inplace=True)
            df.to_csv(dst_path, index=False)
            return dst_path


        transactions = transform_transactions_batches(config.S3_KEY_TRANSACTIONS_BATCH, config.FS_PATH_TRANSACTIONS_INC)
        currencies = transform_currencies_history(config.S3_KEY_CURRENCIES_HISTORY, config.FS_PATH_CURRENCIES_INC)

        file_sensor >> [transactions, currencies]


    @task_group(group_id='LOAD')
    def load(vertica_conn_id: str):

        @task_group(group_id='STAGING')
        def load_staging(fs_conn_id: str):

            file_sensor = (
                FileSensor
                .partial(task_id='file_sensor', fs_conn_id=fs_conn_id)
                .expand(filepath=[
                    '{{ ds_nodash }}_ODS_' + f for f in (config.FS_PATH_TRANSACTIONS_INC, config.FS_PATH_CURRENCIES_INC)
                ])
            )

            def copy_to_vertica(fs_conn_id: str, filename: str, table: str, ds_nodash: str = None):
                fs_hook = FSHook(fs_conn_id)
                full_path = os.path.join(fs_hook.basepath, f'{ds_nodash}_ODS_{filename}')

                df = pd.read_csv(full_path)
                delimiter, lineterminator = '\x1F', '\x1E'
                with NamedTemporaryFile('wb+') as f:
                    df.to_csv(f.name, delimiter, lineterminator=lineterminator, index=False, header=False)

                    vertica_hook = VerticaHook(vertica_conn_id)
                    vertica_hook.run(f"""
                        COPY STV2025021848__STAGING.{table}({', '.join(df.columns)})
                        FROM LOCAL '{f.name}'
                        DELIMITER E'{delimiter}'
                        RECORD TERMINATOR E'{lineterminator}'
                        ABORT ON ERROR;
                    """)

            copy_transactions = PythonOperator(
                task_id='load_transactions',
                python_callable=copy_to_vertica,
                provide_context=True,
                op_args=[fs_conn_id, config.FS_PATH_TRANSACTIONS_INC, config.VERTICA_TABLE_TRANSACTIONS]
            )

            copy_currencies = PythonOperator(
                task_id='load_currencies',
                python_callable=copy_to_vertica,
                provide_context=True,
                op_args=[fs_conn_id, config.FS_PATH_CURRENCIES_INC, config.VERTICA_TABLE_CURRENCIES]
            )

            file_sensor >> [copy_transactions, copy_currencies]
        

        @task_group(group_id='DWH')
        def load_dwh(fs_conn_id: str):
    
            def iter_sql_files(fs_conn_id: str, filepath: str):
                fs_hook = FSHook(fs_conn_id)
                sql_path = Path(fs_hook.basepath)
                yield from sql_path.glob(filepath)

            @task_group(group_id='hubs')
            def load_hubs():  
                insert_hubs = [
                    VerticaOperator(task_id=file.stem, vertica_conn_id=vertica_conn_id, sql=str(file))
                    for file in iter_sql_files(fs_conn_id, 'dwh/hubs/INSERT_*.sql')
                ]

            @task_group(group_id='links')
            def load_links():
                insert_links = [
                    VerticaOperator(task_id=file.stem, vertica_conn_id=vertica_conn_id, sql=str(file))
                    for file in iter_sql_files(fs_conn_id, 'dwh/links/INSERT_*.sql')
                ]

            @task_group(group_id='satellites')
            def load_satellites():
                insert_satellites = [
                    VerticaOperator(task_id=file.stem, vertica_conn_id=vertica_conn_id, sql=str(file))
                    for file in iter_sql_files(fs_conn_id, 'dwh/satellites/INSERT_*.sql')
                ]


            hubs = load_hubs()
            links = load_links()
            satellites = load_satellites()

            hubs >> links >> satellites


        staging = load_staging(config.FS_DATADIR_CONN_ID)
        dwh = load_dwh(config.FS_SQLDIR_CONN_ID)

        staging >> dwh


    E = extract(config.S3_ORIGIN_CONN_ID, config.FS_DATADIR_CONN_ID)
    T = transform(config.FS_DATADIR_CONN_ID)
    L = load(config.VERTICA_WAREHOUSE_CONN_ID)

    E >> T >> L


_ = data_import()
