from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO, SYNC_FROM_CLICKHOUSE_DRIVER_INFO, \
    CLICKHOUSE_SYNC_INTERVAL, CLICKHOUSE_SYNC_TABLE_INCLUDES
from oss_know.libs.clickhouse.sync_clickhouse_data import sync_data_from_remote_ck

clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
sync_from_clickhouse_conn_info = Variable.get(SYNC_FROM_CLICKHOUSE_DRIVER_INFO, deserialize_json=True)

sync_interval = Variable.get(CLICKHOUSE_SYNC_INTERVAL, default_var=None)
tables_to_sync = Variable.get(CLICKHOUSE_SYNC_TABLE_INCLUDES, deserialize_json=True, default_var=[])

# Daily sync common data from other clickhouse environment
with DAG(dag_id='daily_table_data_sync_from_clickhouse',  # schedule_interval='*/5 * * * *',
         schedule_interval=sync_interval, start_date=datetime(2021, 1, 1), catchup=False,
         tags=['daily sync clickhouse'], ) as dag:
    def do_sync_data_from_clickhouse(table_name, timestamp_col):
        sync_data_from_remote_ck(clickhouse_conn_info, sync_from_clickhouse_conn_info, table_name, timestamp_col)


    for table_info in tables_to_sync:
        table_name = table_info['table_name']
        timestamp_col = table_info['timestamp_col']

        op_sync_github_profile_from_clickhouse_group = PythonOperator(
            task_id=f'op_sync_{table_name}_from_clickhouse',
            python_callable=do_sync_data_from_clickhouse,
            op_kwargs={
                "table_name": table_name,
                "timestamp_col": timestamp_col,
            }
        )
