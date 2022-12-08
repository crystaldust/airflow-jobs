from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_ISSUES, \
    OPENSEARCH_INDEX_GITHUB_ISSUES_COMMENTS, OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE
from oss_know.libs.base_dict.variable_key import OPENSEARCH_CONN_DATA, GITHUB_TOKENS, PROXY_CONFS, \
    DAILY_SYNC_GITHUB_ISSUES_EXCLUDES, CK_TABLE_DEFAULT_VAL_TPLT, CLICKHOUSE_DRIVER_INFO, MIN_GITHUB_TOKEN_REMAINING
from oss_know.libs.clickhouse.init_ck_transfer_data import parse_data_init
from oss_know.libs.clickhouse.sync_ck_transfer_data import sync_from_opensearch_to_clickhouse_by_repo
from oss_know.libs.github import sync_issues, sync_issues_comments, sync_issues_timelines
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.github_api import GithubAPI
from oss_know.libs.util.opensearch_api import OpensearchAPI
from oss_know.libs.util.proxy import KuaiProxyService, ProxyManager, GithubTokenProxyAccommodator
from oss_know.libs.util.token import TokenManager

with DAG(
        dag_id='daily_github_issues_sync_v2',
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github', 'daily sync'],
) as dag:
    github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
    proxy_confs = Variable.get(PROXY_CONFS, deserialize_json=True)
    opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
    clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)

    github_api = GithubAPI()


    def scheduler_sync_github_issues(ds, **kwargs):
        min_token_remaining = Variable.get(MIN_GITHUB_TOKEN_REMAINING, default_var=15000)
        token_remaining = github_api.check_token_limits(github_tokens)

        if token_remaining < min_token_remaining:
            raise Exception(
                f"Github token remaining is not enough, remaining: {token_remaining}, min: {min_token_remaining}")

        return 'End:scheduler_sync_github_issues'


    op_scheduler_sync_github_issues = PythonOperator(
        task_id='op_scheduler_sync_github_issues',
        python_callable=scheduler_sync_github_issues
    )


    def get_proxy_accommodator():
        proxy_api_url = proxy_confs["api_url"]
        proxy_order_id = proxy_confs["orderid"]
        proxy_reserved_proxies = proxy_confs["reserved_proxies"]
        proxies = []
        for proxy in proxy_reserved_proxies:
            proxies.append(f"http://{proxy}")
        proxy_service = KuaiProxyService(api_url=proxy_api_url,
                                         orderid=proxy_order_id)
        token_manager = TokenManager(tokens=github_tokens)
        proxy_manager = ProxyManager(proxies=proxies,
                                     proxy_service=proxy_service)
        proxy_accommodator = GithubTokenProxyAccommodator(token_manager=token_manager,
                                                          proxy_manager=proxy_manager,
                                                          shuffle=True,
                                                          policy=GithubTokenProxyAccommodator.POLICY_FIXED_MAP)
        return proxy_accommodator


    def do_sync_opensearch_github_issues(params):
        proxy_accommodator = get_proxy_accommodator()
        owner = params["owner"]
        repo = params["repo"]

        issues_numbers, pr_numbers = sync_issues.sync_github_issues(opensearch_conn_info=opensearch_conn_info,
                                                                    owner=owner,
                                                                    repo=repo,
                                                                    token_proxy_accommodator=proxy_accommodator)

        return issues_numbers


    def do_sync_clickhouse_from_opensearch(opensearch_index, owner, repo):
        templates_var = Variable.get(CK_TABLE_DEFAULT_VAL_TPLT, deserialize_json=True)
        template = None
        for template_var in templates_var:
            if template_var.get("table_name") == opensearch_index:
                template = template_var
                break
        if not template:
            raise Exception(f"Can not find {opensearch_index} table template")

        df = pd.json_normalize(template["temp"])
        parsed_template = parse_data_init(df)

        sync_from_opensearch_to_clickhouse_by_repo(
            owner=owner,
            repo=repo,
            opensearch_conn_info=opensearch_conn_info,
            opensearch_index=opensearch_index,
            clickhouse_conn_info=clickhouse_conn_info,
            table_name=opensearch_index,
            template=parsed_template,
        )
        return f'do_sync_clickhouse_{opensearch_index}:::end'


    def do_sync_clickhouse_github_issues(params):
        templates_var = Variable.get(CK_TABLE_DEFAULT_VAL_TPLT, deserialize_json=True)
        github_pr_template = None
        for template in templates_var:
            if template.get("table_name") == "github_issues":
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
            opensearch_index=OPENSEARCH_INDEX_GITHUB_ISSUES,
            clickhouse_conn_info=clickhouse_conn_info,
            table_name=OPENSEARCH_INDEX_GITHUB_ISSUES,
            template=parsed_template,
        )

        return 'do_sync_clickhouse_github_issues:::end'


    def do_sync_opensearch_github_issues_comments(params, **kwargs):
        owner = params["owner"]
        repo = params["repo"]

        ti = kwargs['ti']
        task_ids = f'op_sync_opensearch_github_issues_{owner}_{repo}'
        issues_numbers = ti.xcom_pull(task_ids=task_ids)

        proxy_accommodator = get_proxy_accommodator()
        do_sync_since = sync_issues_comments.sync_github_issues_comments(
            opensearch_conn_info=opensearch_conn_info,
            owner=owner,
            repo=repo,
            token_proxy_accommodator=proxy_accommodator,
            issues_numbers=issues_numbers
        )


    def do_sync_opensearch_github_issues_timeline(params, **kwargs):
        owner = params["owner"]
        repo = params["repo"]

        ti = kwargs['ti']
        task_ids = f'op_sync_opensearch_github_issues_{owner}_{repo}'
        issues_numbers = ti.xcom_pull(task_ids=task_ids)

        proxy_accommodator = get_proxy_accommodator()

        do_sync_since = sync_issues_timelines.sync_github_issues_timelines(
            opensearch_conn_info=opensearch_conn_info,
            owner=owner,
            repo=repo,
            token_proxy_accommodator=proxy_accommodator,
            issues_numbers=issues_numbers)


    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    opensearch_api = OpensearchAPI()

    excludes = Variable.get(DAILY_SYNC_GITHUB_ISSUES_EXCLUDES, deserialize_json=True, default_var=None)
    uniq_owner_repos = opensearch_api.get_uniq_owner_repos(opensearch_client, OPENSEARCH_INDEX_GITHUB_ISSUES, excludes)
    for uniq_owner_repo in uniq_owner_repos:
        owner = uniq_owner_repo['owner']
        repo = uniq_owner_repo['repo']

        op_sync_opensearch_github_issues = PythonOperator(
            task_id=f'op_sync_opensearch_github_issues_{owner}_{repo}',
            python_callable=do_sync_opensearch_github_issues,
            op_kwargs={'params': uniq_owner_repo},
            provide_context=True,
        )
        op_sync_clickhouse_github_issues = PythonOperator(
            task_id=f'op_sync_clickhouse_github_issues_{owner}_{repo}',
            python_callable=do_sync_clickhouse_from_opensearch,
            op_kwargs={'owner': owner, 'repo': repo, 'opensearch_index': OPENSEARCH_INDEX_GITHUB_ISSUES},
        )

        op_sync_opensearch_github_issues_comments = PythonOperator(
            task_id=f'op_sync_opensearch_github_issues_comments_{owner}_{repo}',
            python_callable=do_sync_opensearch_github_issues_comments,
            op_kwargs={'params': uniq_owner_repo},
            # provide_context=True,
        )
        op_sync_clickhouse_github_issues_comments = PythonOperator(
            task_id=f'op_sync_clickhouse_github_issues_comments_{owner}_{repo}',
            python_callable=do_sync_clickhouse_from_opensearch,
            op_kwargs={'owner': owner, 'repo': repo, 'opensearch_index': OPENSEARCH_INDEX_GITHUB_ISSUES_COMMENTS},
        )

        op_sync_opensearch_github_issues_timeline = PythonOperator(
            task_id=f'op_sync_opensearch_github_issues_timeline_{owner}_{repo}',
            python_callable=do_sync_opensearch_github_issues_timeline,
            op_kwargs={'params': uniq_owner_repo},
            # provide_context=True,
        )
        op_sync_clickhouse_github_issues_timeline = PythonOperator(
            task_id=f'op_sync_clickhouse_github_issues_timeline_{owner}_{repo}',
            python_callable=do_sync_clickhouse_from_opensearch,
            op_kwargs={'owner': owner, 'repo': repo, 'opensearch_index': OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE},
        )

        op_scheduler_sync_github_issues >> op_sync_opensearch_github_issues >> op_sync_clickhouse_github_issues
        op_sync_opensearch_github_issues >> op_sync_opensearch_github_issues_comments >> op_sync_clickhouse_github_issues_comments
        op_sync_opensearch_github_issues >> op_sync_opensearch_github_issues_timeline >> op_sync_clickhouse_github_issues_timeline
