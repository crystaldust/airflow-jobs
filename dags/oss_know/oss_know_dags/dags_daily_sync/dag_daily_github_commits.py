from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_COMMITS
from oss_know.libs.base_dict.variable_key import GITHUB_TOKENS, OPENSEARCH_CONN_DATA, PROXY_CONFS, \
    DAILY_SYNC_GITHUB_COMMITS_EXCLUDES, CLICKHOUSE_DRIVER_INFO, CK_TABLE_DEFAULT_VAL_TPLT, MIN_GITHUB_TOKEN_REMAINING
from oss_know.libs.clickhouse.init_ck_transfer_data import parse_data_init
from oss_know.libs.clickhouse.sync_ck_transfer_data import sync_from_opensearch_to_clickhouse_by_repo
from oss_know.libs.github.sync_commits import sync_github_commits
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.github_api import GithubAPI
from oss_know.libs.util.opensearch_api import OpensearchAPI
from oss_know.libs.util.proxy import KuaiProxyService, ProxyManager, GithubTokenProxyAccommodator
from oss_know.libs.util.token import TokenManager

opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
github_api = GithubAPI()

with DAG(dag_id='daily_github_commits_sync',  # schedule_interval='*/5 * * * *',
         schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False,
         tags=['github', 'daily sync']) as dag:
    github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
    proxy_confs = Variable.get(PROXY_CONFS, deserialize_json=True)
    proxy_api_url = proxy_confs["api_url"]
    proxy_order_id = proxy_confs["orderid"]
    proxy_reserved_proxies = proxy_confs["reserved_proxies"]
    proxies = []
    for proxy in proxy_reserved_proxies:
        proxies.append(f"http://{proxy}")
    proxy_service = KuaiProxyService(api_url=proxy_api_url, orderid=proxy_order_id)
    token_manager = TokenManager(tokens=github_tokens)
    proxy_manager = ProxyManager(proxies=proxies, proxy_service=proxy_service)
    proxy_accommodator = GithubTokenProxyAccommodator(token_manager=token_manager,
                                                      proxy_manager=proxy_manager, shuffle=True,
                                                      policy=GithubTokenProxyAccommodator.POLICY_FIXED_MAP)


    def op_init_daily_github_commits_sync():
        min_token_remaining = Variable.get(MIN_GITHUB_TOKEN_REMAINING, default_var=15000)
        token_remaining = github_api.check_token_limits(github_tokens)

        if token_remaining < min_token_remaining:
            raise Exception(
                f"Github token remaining is not enough, remaining: {token_remaining}, min: {min_token_remaining}")

        return 'Start init_daily_github_commits_sync'


    op_init_daily_github_commits_sync = PythonOperator(task_id='op_init_daily_github_commits_sync',
                                                       python_callable=op_init_daily_github_commits_sync)


    def do_sync_opensearch_github_commits(params):
        owner = params["owner"]
        repo = params["repo"]

        sync_github_commits(opensearch_conn_info, owner, repo, proxy_accommodator)
        return 'do_sync_github_commits:::end'


    def do_sync_clickhouse_github_commits(params):
        templates_var = Variable.get(CK_TABLE_DEFAULT_VAL_TPLT, deserialize_json=True)
        github_commits_template = None
        for template in templates_var:
            if template.get("table_name") == "github_commits":
                github_commits_template = template
                break
        if not github_commits_template:
            raise Exception("Can not find gits table template")

        import pandas as pd
        df = pd.json_normalize(github_commits_template["temp"])
        parsed_template = parse_data_init(df)

        sync_from_opensearch_to_clickhouse_by_repo(
            owner=params["owner"],
            repo=params["repo"],
            opensearch_conn_info=opensearch_conn_info,
            opensearch_index=OPENSEARCH_INDEX_GITHUB_COMMITS,
            clickhouse_conn_info=clickhouse_conn_info,
            table_name=OPENSEARCH_INDEX_GITHUB_COMMITS,
            template=parsed_template,
        )

        return 'do_sync_clickhouse_github_commits:::end'


    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    opensearch_api = OpensearchAPI()

    excludes = Variable.get(DAILY_SYNC_GITHUB_COMMITS_EXCLUDES, deserialize_json=True, default_var=None)
    uniq_owner_repos = opensearch_api.get_uniq_owner_repos(opensearch_client, OPENSEARCH_INDEX_GITHUB_COMMITS, excludes)

    for uniq_item in uniq_owner_repos:
        owner = uniq_item['owner']
        repo = uniq_item['repo']

        op_do_sync_opensearch_github_commits = PythonOperator(
            task_id=f'do_sync_github_commits_{owner}_{repo}',
            python_callable=do_sync_opensearch_github_commits,
            op_kwargs={
                'params': {
                    "owner": owner,
                    "repo": repo,
                }
            })

        op_do_sync_clickhouse_github_commits = PythonOperator(
            task_id=f'do_sync_clickhouse_github_commits_{owner}_{repo}',
            python_callable=do_sync_clickhouse_github_commits,
            op_kwargs={
                'params': {
                    "owner": owner,
                    "repo": repo,
                }
            })

        op_init_daily_github_commits_sync >> op_do_sync_opensearch_github_commits >> op_do_sync_clickhouse_github_commits
