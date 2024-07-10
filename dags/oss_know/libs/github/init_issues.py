import datetime
import random
import time

import requests
from opensearchpy import OpenSearch

from oss_know.libs.base_dict.options import GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX
from oss_know.libs.util.github_api import GithubAPI
from oss_know.libs.util.log import logger
from oss_know.libs.util.opensearch_api import OpensearchAPI


def init_github_issues(opensearch_conn_infos, owner, repo, token_proxy_accommodator, since=None):
    now_time = datetime.datetime.now()
    opensearch_client = OpenSearch(
        hosts=[{'host': opensearch_conn_infos["HOST"], 'port': opensearch_conn_infos["PORT"]}],
        http_compress=True,
        http_auth=(opensearch_conn_infos["USER"], opensearch_conn_infos["PASSWD"]),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )

    session = requests.Session()
    github_api = GithubAPI()
    opensearch_api = OpensearchAPI()
    page = 1
    logger.info(f'Fetching github issues of {owner}/{repo}' + f' since {since}' if since else '')
    while True:
        # Token sleep
        time.sleep(random.uniform(GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX))

        # 获取github issues
        req = github_api.get_github_issues(http_session=session, token_proxy_accommodator=token_proxy_accommodator,
                                           owner=owner, repo=repo, page=page, since=since)
        one_page_github_issues = req.json()

        if not one_page_github_issues:
            logger.info(f"init sync github issues end to break:{owner}/{repo} page_index:{page}")
            break

        # 插入一页 github isuess 到 opensearch
        opensearch_api.bulk_github_issues(opensearch_client=opensearch_client,
                                          github_issues=one_page_github_issues,
                                          owner=owner, repo=repo, if_sync=0)

        logger.info(f"success get github issues page:{owner}/{repo} page_index:{page}")
        page += 1

    # 建立 sync 标志
    opensearch_api.set_sync_github_issues_check(opensearch_client, owner, repo, now_time)
