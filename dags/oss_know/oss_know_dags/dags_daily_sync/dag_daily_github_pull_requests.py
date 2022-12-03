from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS
from oss_know.libs.base_dict.variable_key import GITHUB_TOKENS, OPENSEARCH_CONN_DATA, PROXY_CONFS, \
    DAILY_SYNC_GITHUB_PRS_EXCLUDES, CLICKHOUSE_DRIVER_INFO, CK_TABLE_DEFAULT_VAL_TPLT
from oss_know.libs.clickhouse.init_ck_transfer_data import parse_data_init
from oss_know.libs.clickhouse.sync_ck_transfer_data import sync_from_opensearch_to_clickhouse_by_repo
from oss_know.libs.github.sync_pull_requests import sync_github_pull_requests
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.opensearch_api import OpensearchAPI
from oss_know.libs.util.proxy import KuaiProxyService, ProxyManager, GithubTokenProxyAccommodator
from oss_know.libs.util.token import TokenManager

with DAG(dag_id='daily_github_pull_requests_sync',  # schedule_interval='*/5 * * * *',
         schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False,
         tags=['github', 'daily sync']) as dag:
    def op_init_daily_github_pull_requests_sync():
        return 'Start init_daily_github_pull_requests_sync'


    op_init_daily_github_pull_requests_sync = PythonOperator(task_id='op_init_daily_github_pull_requests_sync',
                                                             python_callable=op_init_daily_github_pull_requests_sync)

    github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
    proxy_confs = Variable.get(PROXY_CONFS, deserialize_json=True)
    opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
    clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
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


    def do_sync_opensearch_github_pull_requests(params):
        owner = params["owner"]
        repo = params["repo"]

        sync_github_pull_requests(opensearch_conn_info, owner, repo, proxy_accommodator)
        return 'do_sync_github_pull_requests:::end'


    def do_sync_clickhouse_github_pull_requests(params):
        templates_var = Variable.get(CK_TABLE_DEFAULT_VAL_TPLT, deserialize_json=True)
        github_pr_template = None
        for template in templates_var:
            if template.get("table_name") == "github_pull_requests":
                github_pr_template = template
                break
        if not github_pr_template:
            raise Exception("Can not find gits table template")

        import pandas as pd
        df = pd.json_normalize(github_pr_template["temp"])
        parsed_template = parse_data_init(df)

        sync_from_opensearch_to_clickhouse_by_repo(
            owner=params.get('owner'),
            repo=params.get('repo'),
            opensearch_conn_info=opensearch_conn_info,
            opensearch_index=OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS,
            clickhouse_conn_info=clickhouse_conn_info,
            table_name=OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS,
            template=parsed_template,
        )

        return 'do_sync_clickhouse_github_pull_requests:::end'


    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    opensearch_api = OpensearchAPI()

    excludes = Variable.get(DAILY_SYNC_GITHUB_PRS_EXCLUDES, deserialize_json=True, default_var=None)
    uniq_owner_repos = opensearch_api.get_uniq_owner_repos(opensearch_client, OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS,
                                                           excludes)
    for uniq_item in uniq_owner_repos:
        owner = uniq_item['owner']
        repo = uniq_item['repo']

        op_sync_opensearch_github_pull_requests = PythonOperator(
            task_id=f'do_sync_opensearch_github_pull_requests_{owner}_{repo}',
            python_callable=do_sync_opensearch_github_pull_requests,
            op_kwargs={
                'params': {
                    "owner": owner,
                    "repo": repo
                }
            })

        op_sync_clickhouse_github_pull_requests = PythonOperator(
            task_id=f'do_sync_clickhouse_github_pull_requests_{owner}_{repo}',
            python_callable=do_sync_clickhouse_github_pull_requests,
            op_kwargs={
                'params': {
                    "owner": owner,
                    "repo": repo
                }
            })
        op_init_daily_github_pull_requests_sync >> op_sync_opensearch_github_pull_requests >> op_sync_clickhouse_github_pull_requests
