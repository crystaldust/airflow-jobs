from datetime import datetime
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_GIT_RAW
from oss_know.libs.base_dict.variable_key import DAILY_SYNC_GITS_EXCLUDES, CK_TABLE_DEFAULT_VAL_TPLT, \
    OPENSEARCH_CONN_DATA, CLICKHOUSE_DRIVER_INFO
from oss_know.libs.clickhouse.sync_ck_transfer_data import sync_from_opensearch_to_clickhouse_by_repo
from oss_know.libs.clickhouse.init_ck_transfer_data import parse_data_init

from oss_know.libs.util.opensearch_api import OpensearchAPI
from oss_know.libs.github.sync_gits import sync_git_datas
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable

opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)

with DAG(dag_id='daily_gits_sync',  # schedule_interval='*/5 * * * *',
         schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False,
         tags=['github', 'daily sync'], ) as dag:
    def op_init_daily_gits_sync():
        return 'Start init_daily_gits_sync'


    op_init_daily_gits_sync = PythonOperator(task_id='op_init_daily_gits_sync',
                                             python_callable=op_init_daily_gits_sync, )


    def do_sync_opensearch_gits_data(params):
        owner = params["owner"]
        repo = params["repo"]
        url = params["url"]
        proxy_config = params.get("proxy_config")
        git_save_local_path = Variable.get("git_save_local_path", deserialize_json=True)
        sync_git_datas(url, owner=owner, repo=repo, proxy_config=proxy_config,
                       opensearch_conn_datas=opensearch_conn_info, git_save_local_path=git_save_local_path)
        return 'do_sync_git_info:::end'


    def do_sync_clickhouse_gits_data(params):
        templates_var = Variable.get(CK_TABLE_DEFAULT_VAL_TPLT, deserialize_json=True)
        gits_template = None
        for template in templates_var:
            if template.get("table_name") == "gits":
                gits_template = template
                break
        if not gits_template:
            raise Exception("Can not find gits table template")

        import pandas as pd
        df = pd.json_normalize(gits_template["temp"])
        parsed_template = parse_data_init(df)

        sync_from_opensearch_to_clickhouse_by_repo(
            owner=params["owner"],
            repo=params["repo"],
            opensearch_conn_info=opensearch_conn_info,
            opensearch_index=OPENSEARCH_GIT_RAW,
            clickhouse_conn_info=clickhouse_conn_info,
            table_name=OPENSEARCH_GIT_RAW,
            template=parsed_template,
        )


    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    opensearch_api = OpensearchAPI()

    excludes = Variable.get(DAILY_SYNC_GITS_EXCLUDES, deserialize_json=True, default_var=None)
    uniq_owner_repos = opensearch_api.get_uniq_owner_repos(opensearch_client, OPENSEARCH_GIT_RAW, excludes)
    for uniq_item in uniq_owner_repos:
        owner = uniq_item['owner']
        repo = uniq_item['repo']
        origin = uniq_item['origin']

        op_sync_opensearch_gits_data = PythonOperator(task_id=f'do_sync_opensearch_gits_data_{owner}_{repo}',
                                                      python_callable=do_sync_opensearch_gits_data,
                                                      op_kwargs={
                                                          'params': {
                                                              "url": origin,
                                                              "owner": owner,
                                                              "repo": repo,
                                                              "proxy_config": None,
                                                          }
                                                      })

        op_sync_clickhouse_gits_data = PythonOperator(task_id=f'do_sync_clickhouse_gits_data_{owner}_{repo}',
                                                      python_callable=do_sync_clickhouse_gits_data,
                                                      op_kwargs={
                                                          'params': {
                                                              "owner": owner,
                                                              "repo": repo,
                                                          }
                                                      })

        op_init_daily_gits_sync >> op_sync_opensearch_gits_data >> op_sync_clickhouse_gits_data
