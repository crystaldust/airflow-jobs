from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE
from oss_know.libs.base_dict.variable_key import OPENSEARCH_SYNC_INTERVAL, OPENSEARCH_SYNC_COMBINATION_TYPE, \
    SYNC_FROM_OPENSEARCH_CONN_INFO, OPENSEARCH_CONN_DATA
from oss_know.libs.util.base import arrange_owners_into_letter_groups, get_opensearch_client
from oss_know.libs.util.opensearch_api import OpensearchAPI

sync_from_opensearch_conn_info = Variable.get(SYNC_FROM_OPENSEARCH_CONN_INFO, deserialize_json=True)
sync_interval = Variable.get(OPENSEARCH_SYNC_INTERVAL, default_var=None)
sync_combination_type = Variable.get(OPENSEARCH_SYNC_COMBINATION_TYPE, default_var="diff_remote")

opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
remote_opensearch_conn_info = Variable.get(SYNC_FROM_OPENSEARCH_CONN_INFO, deserialize_json=True)
opensearch_api = OpensearchAPI()

all_owners = opensearch_api.get_uniq_owners(get_opensearch_client(opensearch_conn_info),
                                            index=OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE, owners_aggs_size=3000)

# Daily sync gits data from other opensearch environment by owner/repo
with DAG(dag_id='github_issues_timeline_sync_from_opensearch_group_by_owners',  # schedule_interval='*/5 * * * *',
         schedule_interval=sync_interval, start_date=datetime(2021, 1, 1), catchup=False,
         tags=['github', 'sync opensearch'], ) as dag:
    def do_sync_github_issues_timeline_from_opensearch_by_group(owners):
        opensearch_api.sync_from_remote_by_owners(opensearch_conn_info, sync_from_opensearch_conn_info, owners,
                                                  OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE, sync_combination_type)


    # Init 26 sub groups by letter(to make the task DAG static)
    # Split all tasks into 26 groups by their capital letter, all tasks inside a group are executed sequentially
    # To avoid to many parallel tasks and keep the DAG static
    task_groups_by_capital_letter = arrange_owners_into_letter_groups(all_owners)
    prev_group = None
    for letter, owners in task_groups_by_capital_letter.items():
        op_sync_github_issues_timeline_from_opensearch_group = PythonOperator(
            task_id=f'op_sync_github_issues_timeline_from_opensearch_group_{letter}',
            python_callable=do_sync_github_issues_timeline_from_opensearch_by_group,
            trigger_rule='all_done',
            op_kwargs={
                "owners": owners
            }
        )
        if prev_group:
            prev_group >> op_sync_github_issues_timeline_from_opensearch_group
        prev_group = op_sync_github_issues_timeline_from_opensearch_group
