from datetime import datetime
from string import Template

import psycopg2
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.variable_key import OPENSEARCH_CONN_DATA, GITHUB_TOKENS, PROXY_CONFS
# git_init_sync_v0.0.3
from oss_know.libs.util.log import logger
from oss_know.libs.util.proxy import KuaiProxyService, ProxyManager, GithubTokenProxyAccommodator
from oss_know.libs.util.token import TokenManager

with DAG(
        dag_id='git_track_repo',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def init_track_repo():
        return 'Start init_track_repo'


    op_init_track_repo = PythonOperator(
        task_id='init_track_repo',
        python_callable=init_track_repo,
    )

    git_save_local_path = Variable.get("git_save_local_path", deserialize_json=True)
    github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
    opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)

    proxy_confs = Variable.get(PROXY_CONFS, deserialize_json=True)
    proxies = [f'http://{line}' for line in proxy_confs['reserved_proxies']]
    proxy_service = KuaiProxyService(proxy_confs['api_url'], proxy_confs['orderid'])
    proxy_manager = ProxyManager(proxies, proxy_service)
    token_manager = TokenManager(github_tokens)
    proxy_accommodator = GithubTokenProxyAccommodator(token_manager, proxy_manager, shuffle=True,
                                                      policy=GithubTokenProxyAccommodator.POLICY_FIXED_MAP)

    pg_conn = psycopg2.connect(host='192.168.1.2', port='5432', database='airflow', user='airflow', password='airflow')
    pg_cur = pg_conn.cursor()


    def do_init_gits(callback, **kwargs):
        from oss_know.libs.github import init_gits
        owner = kwargs['dag_run'].conf.get('owner')
        repo = kwargs['dag_run'].conf.get('repo')
        url = kwargs['dag_run'].conf.get('url')
        gits_args = [url, owner, repo, None, opensearch_conn_info, git_save_local_path]
        exec_job_and_update_db(init_gits.init_sync_git_datas, callback, 'gits', args=gits_args, **kwargs)


    def do_init_github_commits(callback, **kwargs):
        from oss_know.libs.github import init_commits
        until = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        additional_args = [until]
        exec_job_and_update_db(init_commits.init_github_commits, callback, 'github_commits', additional_args, **kwargs)


    def do_init_github_issues(callback, **kwargs):
        from oss_know.libs.github import init_issues_timeline
        exec_job_and_update_db(init_issues_timeline.init_sync_github_issues_timeline, callback, 'github_issues',
                               **kwargs)


    def do_init_github_issues_comments(callback, **kwargs):
        from oss_know.libs.github import init_issues_comments
        exec_job_and_update_db(init_issues_comments.init_github_issues_comments, callback, 'github_issues_comments',
                               **kwargs)


    def do_init_github_issues_timeline(callback, **kwargs):
        from oss_know.libs.github import init_issues_timeline
        exec_job_and_update_db(init_issues_timeline.init_sync_github_issues_timeline, callback,
                               'github_issues_timeline', **kwargs)


    def do_init_github_pull_requests(callback, **kwargs):
        from oss_know.libs.github import init_pull_requests
        exec_job_and_update_db(init_pull_requests.init_sync_github_pull_requests, callback,
                               'github_pull_requests', **kwargs)


    def exec_job_and_update_db(job_callable, callback, res_type, additional_args=[], args=[], **kwargs):
        owner = None
        repo = None
        url = None
        try:
            owner = kwargs['dag_run'].conf.get('owner')
            repo = kwargs['dag_run'].conf.get('repo')
            url = kwargs['dag_run'].conf.get('url')
        except KeyError:
            logger.error(f'Repo info not found in dag run config, init dag from Variable')

        update_status_template_str = \
            f"""update triggered_git_repos 
            set {res_type}_status=$status 
            where owner='{owner}' and repo='{repo}'"""
        update_status_template = Template(update_status_template_str)

        if 'github.com' not in url:
            logger.info('Not github repository, skip')
            pg_cur.execute(update_status_template.substitute(status=2))
            pg_conn.commit()
            callback(owner, repo, res_type, None)
            return

        since = '1970-01-01T00:00:00Z'
        try:
            pg_cur.execute(update_status_template.substitute(status=1))
            pg_conn.commit()
            if args:
                job_callable(*args)
            else:
                job_args = [opensearch_conn_info, owner, repo, proxy_accommodator, since] + additional_args
                job_callable(*job_args)
            pg_cur.execute(update_status_template.substitute(status=2))
            pg_conn.commit()

            callback(owner, repo, res_type, None)
        except Exception as e:
            pg_cur.execute(update_status_template.substitute(status=3))
            pg_conn.commit()
            callback(owner, repo, res_type, e)


    def job_callback(owner, repo, res_type, e: Exception):
        logger.info(f'Finish job on {owner}/{repo} {res_type}, exception:', e)
        job_status = 'failed' if e else 'success'
        update_status_sql = f"""update triggered_git_repos 
                        set job_status='{job_status}' 
                        where owner='{owner}' and repo='{repo}'"""

        if e:
            pg_cur.execute(update_status_sql)
            pg_conn.commit()
            raise e

        get_statuses_sq = f"""
        select gits_status,
               github_commits_status,
               github_pull_requests_status,
               github_issues_status,
               github_issues_comments_status,
               github_issues_timeline_status
        from triggered_git_repos
        where owner='{owner}' and repo='{repo}'"""
        pg_cur.execute(get_statuses_sq)
        records = pg_cur.fetchall()
        if not records:  # or records' length is not 1
            raise Exception(f'Failed to check {owner}/{repo} job status, record not found')

        job_finished = True
        for status in records[0]:
            if status != 2:
                job_finished = False
        if job_finished:
            pg_cur.execute(update_status_sql)
            pg_conn.commit()


    op_do_init_gits = PythonOperator(
        task_id=f'do_init_gits',
        python_callable=do_init_gits,
        provide_context=True,
        op_kwargs={'callback': job_callback}
    )

    op_do_init_github_commits = PythonOperator(
        task_id=f'do_init_github_commits',
        python_callable=do_init_github_commits,
        provide_context=True,
        op_kwargs={'callback': job_callback}
    )

    op_do_init_github_issues = PythonOperator(
        task_id=f'do_init_github_issues',
        python_callable=do_init_github_issues,
        provide_context=True,
        op_kwargs={'callback': job_callback}
    )

    op_do_init_github_issues_comments = PythonOperator(
        task_id=f'do_init_github_issues_comments',
        python_callable=do_init_github_issues_comments,
        provide_context=True,
        op_kwargs={'callback': job_callback}
    )

    op_do_init_github_issues_timeline = PythonOperator(
        task_id=f'do_init_github_issues_timeline',
        python_callable=do_init_github_issues_timeline,
        provide_context=True,
        op_kwargs={'callback': job_callback}
    )

    op_do_init_github_pull_requests = PythonOperator(
        task_id=f'do_init_github_pull_requests',
        python_callable=do_init_github_pull_requests,
        provide_context=True,
        op_kwargs={'callback': job_callback}
    )

    op_init_track_repo >> op_do_init_gits
    op_init_track_repo >> op_do_init_github_commits
    op_init_track_repo >> op_do_init_github_pull_requests
    op_init_track_repo >> op_do_init_github_issues >> [op_do_init_github_issues_comments,
                                                       op_do_init_github_issues_timeline]
