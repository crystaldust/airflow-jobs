import random
import time

import requests
from opensearchpy import OpenSearch

# from .init_issues_timeline import get_github_issues_timeline
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE
from oss_know.libs.base_dict.options import GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX
from oss_know.libs.util.github_api import GithubAPI
from oss_know.libs.util.log import logger
from oss_know.libs.util.opensearch_api import OpensearchAPI


def sync_github_issues_timelines(opensearch_conn_info,
                                 owner,
                                 repo,
                                 token_proxy_accommodator,
                                 issues_numbers):
    logger.info(f'{owner}/{repo} sync timeline of issues {issues_numbers}')
    opensearch_client = OpenSearch(
        hosts=[{'host': opensearch_conn_info["HOST"], 'port': opensearch_conn_info["PORT"]}],
        http_compress=True,
        http_auth=(opensearch_conn_info["USER"], opensearch_conn_info["PASSWD"]),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )
    http_session = requests.Session()
    opensearch_api = OpensearchAPI()
    github_api = GithubAPI()

    for issues_number in issues_numbers:
        # 同步前删除原来存在的issues_numbers对应timeline
        del_result = opensearch_client.delete_by_query(index=OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE,
                                                       body={
                                                           "query": {
                                                               "bool": {
                                                                   "must": [
                                                                       {
                                                                           "term": {
                                                                               "search_key.owner.keyword": {
                                                                                   "value": owner
                                                                               }
                                                                           }
                                                                       },
                                                                       {
                                                                           "term": {
                                                                               "search_key.repo.keyword": {
                                                                                   "value": repo
                                                                               }
                                                                           }
                                                                       },
                                                                       {
                                                                           "term": {
                                                                               "search_key.number": {
                                                                                   "value": issues_number
                                                                               }
                                                                           }
                                                                       }
                                                                   ]
                                                               }
                                                           }
                                                       })
        logger.info(f"DELETE github issues {issues_number} timeline result:{del_result}")

        page = 1
        while True:
            # Token sleep
            time.sleep(random.uniform(GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX))
            req = github_api.get_github_issues_timeline(
                http_session=http_session,
                token_proxy_accommodator=token_proxy_accommodator,
                owner=owner,
                repo=repo,
                number=issues_number,
                page=page)
            one_page_github_issues_timeline = req.json()

            if not one_page_github_issues_timeline:
                logger.info(f"All github issues timeline fetched:{owner}/{repo}/{issues_number} page_index:{page}")
                break

            opensearch_api.bulk_github_issues_timeline(opensearch_client=opensearch_client,
                                                       issues_timelines=one_page_github_issues_timeline,
                                                       owner=owner, repo=repo, number=issues_number, if_sync=1)

            logger.info(f"success get github issues timeline page:{owner}/{repo}/{issues_number} page_index:{page}")
            page += 1
