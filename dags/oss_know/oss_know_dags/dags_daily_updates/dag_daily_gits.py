import json
from datetime import datetime
# from oss_know.libs.maillist.archive import sync_archive
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.opensearch_api import OpensearchAPI
from oss_know.libs.github.sync_gits import sync_git_datas
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable

# v0.0.1 It is a mailing list DAG


opensearch_conn_info = Variable.get("opensearch_conn_data", deserialize_json=True)

GIT_REPO_URL_MAP = {
    'gnu___emacs': 'https://git.savannah.gnu.org/git/emacs.git'
}

with DAG(
        dag_id='daily_gits_sync',
        # schedule_interval='*/5 * * * *',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def op_init_daily_gits_sync():
        return 'Start init_daily_gits_sync'


    op_init_daily_gits_sync = PythonOperator(
        task_id='op_init_daily_gits_sync',
        python_callable=op_init_daily_gits_sync,
    )


    def do_sync_git_info(params):
        from airflow.models import Variable
        owner = params["owner"]
        repo = params["repo"]
        url = params["url"]
        proxy_config = params.get("proxy_config")
        opensearch_conn_datas = Variable.get("opensearch_conn_data", deserialize_json=True)
        git_save_local_path = Variable.get("git_save_local_path", deserialize_json=True)
        sync_git_datas(url, owner=owner, repo=repo, proxy_config=proxy_config,
                       opensearch_conn_datas=opensearch_conn_datas, git_save_local_path=git_save_local_path)
        return 'do_sync_git_info:::end'


    opensearch_client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_info)
    opensearch_api = OpensearchAPI()

    uniq_owner_repos = opensearch_api.get_uniq_owner_repos(opensearch_client, 'gits')
    for owner, repo in uniq_owner_repos:
        # sync_git_datas(f'https://github.com/{owner}/{repo}', owner, repo, None, opensearch_conn_info)
        # git_repo_url = GIT_REPO_URL_MAP[f'{owner}___{repo}']
        git_repo_url = f'https://github.com/{owner}/{repo}'

        op_do_init_sync_git_info = PythonOperator(
            task_id=f'do_sync_git_info_{owner}_{repo}',
            python_callable=do_sync_git_info,
            op_kwargs={'params': {
                "url": git_repo_url,
                "owner": owner,
                "repo": repo,
                "proxy_config": None,
            }},
        )
        op_init_daily_gits_sync >> op_do_init_sync_git_info

# Sync (owner, repo) one by one
# Start AirFlow DAG with customized configs(with owner, repo injected)
