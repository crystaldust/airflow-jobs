from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE
from oss_know.libs.base_dict.variable_key import NEED_INIT_GITHUB_ISSUES_TIMELINE_REPOS, GITHUB_TOKENS, \
    OPENSEARCH_CONN_DATA, PROXY_CONFS, CK_TABLE_DEFAULT_VAL_TPLT, CLICKHOUSE_DRIVER_INFO
from oss_know.libs.util.data_transfer import sync_clickhouse_from_opensearch
from oss_know.libs.util.proxy import GithubTokenProxyAccommodator, ProxyServiceProvider, \
    make_accommodator

# v0.0.1
with DAG(
        dag_id='github_init_issues_timeline_v1',
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
    clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)

    github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
    proxy_confs = Variable.get(PROXY_CONFS, deserialize_json=True)
    proxy_accommodator = make_accommodator(github_tokens, proxy_confs, ProxyServiceProvider.Kuai,
                                           GithubTokenProxyAccommodator.POLICY_FIXED_MAP)


    def do_init_github_issues_timeline(owner, repo, since=None):
        init_issues_timeline.init_sync_github_issues_timeline(opensearch_conn_info, owner, repo,
                                                              proxy_accommodator, since)


    def do_ck_transfer(owner, repo, since=None):
        table_templates = Variable.get(CK_TABLE_DEFAULT_VAL_TPLT, deserialize_json=True)
        template = table_templates.get(OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE)

        sync_clickhouse_from_opensearch(owner, repo,
                                        OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE, opensearch_conn_info,
                                        OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE, clickhouse_server_info,
                                        template,
                                        since)


    need_do_init_ops = []

    from airflow.models import Variable

    github_issues_timeline_repos = Variable.get(NEED_INIT_GITHUB_ISSUES_TIMELINE_REPOS,
                                                deserialize_json=True)

    for timeline_owner_repo in github_issues_timeline_repos:
        owner = timeline_owner_repo["owner"]
        repo = timeline_owner_repo["repo"]
        since = timeline_owner_repo.get('since')

        if since:
            # Validate since before execution
            datetime.strptime(since, '%Y-%m-%dT%H:%M:%SZ')

        op_do_init_github_issues_timeline = PythonOperator(
            task_id=f'do_init_github_issues_timeline_{owner}_{repo}',
            python_callable=do_init_github_issues_timeline,
            op_kwargs={
                'owner': owner,
                'repo': repo,
                'since': since
            },
        )

        op_transfer_data_to_ck = PythonOperator(
            task_id=f'do_transfer_data_to_ck_{owner}_{repo}',
            python_callable=do_ck_transfer,
            op_kwargs={
                'owner': owner,
                'repo': repo,
                'since': since
            },
        )

        op_do_init_github_issues_timeline >> op_transfer_data_to_ck
