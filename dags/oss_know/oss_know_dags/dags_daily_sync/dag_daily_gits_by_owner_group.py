from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_GIT_RAW
from oss_know.libs.base_dict.variable_key import CK_TABLE_DEFAULT_VAL_TPLT, OPENSEARCH_CONN_DATA, \
    CLICKHOUSE_DRIVER_INFO, GIT_SAVE_LOCAL_PATH, \
    DAILY_GITS_SYNC_INTERVAL, DAILY_SYNC_INTERVAL
from oss_know.libs.github.sync_gits import sync_gits_opensearch_of_owner
from oss_know.libs.util.base import get_opensearch_client, arrange_owners_into_letter_groups
from oss_know.libs.util.data_transfer import sync_clickhouse_from_opensearch
from oss_know.libs.util.opensearch_api import OpensearchAPI

opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
table_templates = Variable.get(CK_TABLE_DEFAULT_VAL_TPLT, deserialize_json=True)
gits_table_template = table_templates.get(OPENSEARCH_GIT_RAW)
git_save_local_path = Variable.get(GIT_SAVE_LOCAL_PATH, deserialize_json=True)

sync_interval = Variable.get(DAILY_GITS_SYNC_INTERVAL, default_var=None)
if not sync_interval:
    sync_interval = Variable.get(DAILY_SYNC_INTERVAL, default_var=None)

with DAG(dag_id='daily_gits_sync_by_owner_group',  # schedule_interval='*/5 * * * *',
         schedule_interval=sync_interval, start_date=datetime(2021, 1, 1), catchup=False,
         tags=['github', 'daily sync'], ) as dag:
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    opensearch_api = OpensearchAPI()


    def do_sync_gits_opensearch_group(owners, group_letter, ti, proxy_config=None):
        synced_owner_repos = []
        for owner in owners:
            synced_repos = sync_gits_opensearch_of_owner(owner=owner, opensearch_conn_info=opensearch_conn_info,
                                                         proxy_config=proxy_config,
                                                         git_save_local_path=git_save_local_path)
            synced_owner_repos += synced_repos
        ti.xcom_push(key=f'group_{group_letter}_synced_owner_repos', value=synced_owner_repos)


    def do_sync_git_clickhouse_group(group_letter, ti):
        owner_repos = ti.xcom_pull(key=f'group_{group_letter}_synced_owner_repos')
        for owner_repo in owner_repos:
            owner = owner_repo['owner']
            repo = owner_repo['repo']
            sync_clickhouse_from_opensearch(owner, repo,
                                            OPENSEARCH_GIT_RAW, opensearch_conn_info,
                                            OPENSEARCH_GIT_RAW, clickhouse_conn_info, gits_table_template)


    uniq_owners = opensearch_api.get_uniq_owners(opensearch_client, OPENSEARCH_GIT_RAW)
    task_groups_by_capital_letter = arrange_owners_into_letter_groups(uniq_owners)
    # TODO Currently the DAG makes 27 parallel task groups(serial execution inside each group)
    #  Check in production env if it works as expected(won't make too much pressure on opensearch and
    #  clickhouse. Another approach is to make all groups serial, one after another, which assign
    #  init_op to None at the beginning, then assign op_sync_gits_clickhouse_group to prev_op
    #  in each loop iteration(as commented below)
    #  Another tip: though the groups dict is by default sorted by alphabet, the generated DAG won't
    #  respect the order
    # prev_op = None
    for letter, owners in task_groups_by_capital_letter.items():
        op_sync_gits_opensearch_group = PythonOperator(
            task_id=f'op_sync_gits_opensearch_group_{letter}',
            python_callable=do_sync_gits_opensearch_group,
            trigger_rule='all_done',
            op_kwargs={
                "owners": owners,
                "group_letter": letter,
            }
        )
        op_sync_gits_clickhouse_group = PythonOperator(
            task_id=f'op_sync_gits_clickhouse_group_{letter}',
            python_callable=do_sync_git_clickhouse_group,
            trigger_rule='all_done',
            op_kwargs={
                "group_letter": letter
            }
        )
        op_sync_gits_opensearch_group >> op_sync_gits_clickhouse_group
        # prev_op = op_sync_gits_clickhouse_group
