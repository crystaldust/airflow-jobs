import json

import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
# clickhouse_init_sync_v0.0.1
from oss_know.libs.base_dict.variable_key import NEED_CK_TABLE_INFOS, CLICKHOUSE_DRIVER_INFO, \
    TRANSFER_DATA_OS_INDEX_AND_CK_TBNAME

with DAG(
        dag_id='init_ck_transfer_data',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['clickhouse'],
) as dag:
    def init_clickhouse_transfer_data(ds, **kwargs):
        return 'Start init_clickhouse_transfer_data'


    op_init_clickhouse_transfer_data = PythonOperator(
        task_id='init_clickhouse_transfer_data',
        python_callable=init_clickhouse_transfer_data,
    )


    def do_ck_transfer_data(params):
        from airflow.models import Variable
        from oss_know.libs.clickhouse import init_ck_transfer_data
        opensearch_index = params["OPENSEARCH_INDEX"]
        table_name = params["CK_TABLE_NAME"]
        opensearch_conn_datas = Variable.get("opensearch_conn_data", deserialize_json=True)
        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        table_temples = Variable.get("clickhouse_tb_temp", deserialize_json=True)
        # temp = {}
        for table_temple in table_temples:
            if table_temple.get("table_name") == table_name:
                temp = table_temple.get("temp")
                break
        df = pd.json_normalize(temp)
        temp = init_ck_transfer_data.parse_data_init(df)
        # print(json.dumps(temp))
        # raise Exception("我想看看初始化的结果")
        if table_name == 'github_issues_timeline':
            transfer_data = init_ck_transfer_data.transfer_data_special(clickhouse_server_info=clickhouse_server_info,
                                                                        opensearch_index=opensearch_index,
                                                                        table_name=table_name,
                                                                        opensearch_conn_datas=opensearch_conn_datas)
        else:

            transfer_data = init_ck_transfer_data.transfer_data(clickhouse_server_info=clickhouse_server_info,
                                                                opensearch_index=opensearch_index,
                                                                table_name=table_name,
                                                                opensearch_conn_datas=opensearch_conn_datas,
                                                                temp=temp)
        return 'do_ck_transfer_data:::end'


    from airflow.models import Variable

    os_index_ck_tb_infos = Variable.get(TRANSFER_DATA_OS_INDEX_AND_CK_TBNAME, deserialize_json=True)
    for os_index_ck_tb_info in os_index_ck_tb_infos:
        op_do_ck_transfer_data = PythonOperator(
            task_id=f'do_ck_transfer_os_index_{os_index_ck_tb_info["OPENSEARCH_INDEX"]}_ck_tb_{os_index_ck_tb_info["CK_TABLE_NAME"]}',
            python_callable=do_ck_transfer_data,
            op_kwargs={'params': os_index_ck_tb_info},
        )

        op_init_clickhouse_transfer_data >> op_do_ck_transfer_data
