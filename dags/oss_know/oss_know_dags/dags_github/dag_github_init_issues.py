from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_ISSUES
from oss_know.libs.base_dict.variable_key import NEED_INIT_GITHUB_ISSUES_REPOS, PROXY_CONFS, \
    OPENSEARCH_CONN_DATA, GITHUB_TOKENS, CK_TABLE_DEFAULT_VAL_TPLT, CLICKHOUSE_DRIVER_INFO
from oss_know.libs.clickhouse import init_ck_transfer_data
from oss_know.libs.github import init_issues
from oss_know.libs.util.proxy import GithubTokenProxyAccommodator, make_accommodator, \
    ProxyServiceProvider

# v0.0.1
with DAG(
        dag_id='github_init_issues_v1',
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


    def do_init_github_issues(params):
        owner = params["owner"]
        repo = params["repo"]
        since = None
        init_issues.init_github_issues(opensearch_conn_info, owner, repo, proxy_accommodator, since)


    def do_ck_transfer(owner, repo):
        search_key = {"owner": owner, "repo": repo}
        table_templates = Variable.get(CK_TABLE_DEFAULT_VAL_TPLT, deserialize_json=True)

        template = table_templates.get(OPENSEARCH_INDEX_GITHUB_ISSUES)
        init_ck_transfer_data.transfer_data_by_repo(
            clickhouse_server_info=clickhouse_server_info,
            opensearch_index=OPENSEARCH_INDEX_GITHUB_ISSUES,
            table_name=OPENSEARCH_INDEX_GITHUB_ISSUES,
            opensearch_conn_datas=opensearch_conn_info,
            template=template, owner_repo_or_project_maillist_name=search_key,
            transfer_type='github_git_init_by_repo')


    github_issues_repos = Variable.get(NEED_INIT_GITHUB_ISSUES_REPOS, deserialize_json=True)

    for github_issues_owner_repo in github_issues_repos:
        owner = github_issues_owner_repo["owner"]
        repo = github_issues_owner_repo["repo"]

        op_do_init_github_issues = PythonOperator(
            task_id=f'do_init_github_issues_{owner}_{repo}',
            python_callable=do_init_github_issues,
            op_kwargs={'params': github_issues_owner_repo},
        )

        op_transfer_data_to_ck = PythonOperator(
            task_id=f'do_transfer_data_to_ck_{owner}_{repo}',
            python_callable=do_ck_transfer,
            op_kwargs={
                'owner': owner,
                'repo': repo,
            },
        )

        op_do_init_github_issues >> op_transfer_data_to_ck
