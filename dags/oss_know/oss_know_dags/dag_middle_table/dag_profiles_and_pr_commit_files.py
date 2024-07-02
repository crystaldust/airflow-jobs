# company_email_domain_map

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from opensearchpy import OpenSearch

from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO, OPENSEARCH_CONN_DATA, GITHUB_TOKENS, \
    PROXY_CONFS
from oss_know.libs.middle_tables.pr_commits_and_profile import get_pr_commit_files_and_profile_from_logins, \
    insert_company_contributor_profile, insert_company_contributor_pr
from oss_know.libs.util.clickhouse_driver import CKServer
from oss_know.libs.util.proxy import ProxyServiceProvider, GithubTokenProxyAccommodator, make_accommodator

clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
ck_client = CKServer(host=clickhouse_conn_info.get("HOST"),
                     port=clickhouse_conn_info.get("PORT"),
                     user=clickhouse_conn_info.get("USER"),
                     password=clickhouse_conn_info.get("PASSWD"),
                     database=clickhouse_conn_info.get("DATABASE"),
                     settings={
                         "max_execution_time": 1000000,
                     },
                     kwargs={
                         "connect_timeout": 200,
                         "send_receive_timeout": 6000,
                         "sync_request_timeout": 100,
                     })
opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
os_client = OpenSearch(
    hosts=[{'host': opensearch_conn_info["HOST"], 'port': opensearch_conn_info["PORT"]}],
    http_compress=True,
    http_auth=(opensearch_conn_info["USER"], opensearch_conn_info["PASSWD"]),
    use_ssl=True,
    verify_certs=False,
    ssl_assert_hostname=False,
    ssl_show_warn=False,
    timeout=180,
    retry_on_timeout=True
)

github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
proxy_confs = Variable.get(PROXY_CONFS, deserialize_json=True)
proxy_accommodator = make_accommodator(github_tokens, proxy_confs, ProxyServiceProvider.Kuai,
                                       GithubTokenProxyAccommodator.POLICY_FIXED_MAP)

with DAG(dag_id='dag_profiles_and_pr_commit_files',  # schedule_interval='*/5 * * * *',
         schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False,
         tags=['github', 'middle table'], ) as dag:
    def do_get_pr_commit_files_and_profile_from_logins():
        sql = '''
        select from_login
        from (
                 select from_login, count() as event_count
                 from pr_social_events
                 where owner = 'apache'
                   and repo = 'arrow'
                   and from_login not in ['github-actions[bot]']
                   and to_login not in ['github-actions[bot]']
                 group by from_login
                 order by event_count desc
                 )
        where event_count > 50
        '''
        result = ck_client.execute_no_params(sql)
        logins = [t[0] for t in result]
        get_pr_commit_files_and_profile_from_logins(os_client, logins, proxy_accommodator)


    def do_insert_company_contributor_profile():
        insert_company_contributor_profile(os_client, ck_client)


    def do_insert_company_contributor_pr():
        insert_company_contributor_pr(os_client, ck_client)


    op_get_graphql_data = PythonOperator(
        task_id=f'op_get_graphql_data',
        python_callable=do_get_pr_commit_files_and_profile_from_logins,
        trigger_rule='all_done',
        op_kwargs={}
    )

    op_insert_company_contributor_profile = PythonOperator(
        task_id=f'op_insert_company_contributor_profile',
        python_callable=do_insert_company_contributor_profile,
        trigger_rule='all_done',
        op_kwargs={
            "os_client": os_client,
            "ck_client": ck_client,
        }
    )

    op_insert_company_contributor_pr = PythonOperator(
        task_id=f'op_insert_company_contributor_pr',
        python_callable=do_insert_company_contributor_pr,
        trigger_rule='all_done',
        op_kwargs={
            "os_client": os_client,
            "ck_client": ck_client,
        }
    )

    op_get_graphql_data >> op_insert_company_contributor_profile
    op_get_graphql_data >> op_insert_company_contributor_pr
