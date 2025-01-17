from datetime import datetime
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from opensearchpy import OpenSearch
from opensearchpy import helpers as opensearch_helpers

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_ISSUES, \
    OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE
from oss_know.libs.base_dict.options import GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX
from oss_know.libs.exceptions import GithubResourceNotFoundError
from oss_know.libs.util.github_api import GithubAPI
from oss_know.libs.util.log import logger
from oss_know.libs.util.opensearch_api import OpensearchAPI


def init_sync_github_issues_timeline(opensearch_conn_info, owner, repo, token_proxy_accommodator, since=None):
    opensearch_client = OpenSearch(
        hosts=[{'host': opensearch_conn_info["HOST"], 'port': opensearch_conn_info["PORT"]}],
        http_compress=True,
        http_auth=(opensearch_conn_info["USER"], opensearch_conn_info["PASSWD"]),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )
    # 根据指定的 owner/repo , 获取现在所有的 issues，并根据所有 issues 便利相关的 timeline
    query_body = {
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
                    }
                ]
            }
        }
    }
    if since:
        # Try to validate since with format, if parse fails, just let the error throw to stop the operation
        datetime.strptime(since, '%Y-%m-%dT%H:%M:%SZ')

        query_body['query']['bool']['must'].append({
            "range": {
                "raw_data.created_at": {
                    "gte": since
                }
            }
        })
    logger.info(f'Getting issues for issues timeline with OpenSearch query {query_body}')
    scan_results = opensearch_helpers.scan(opensearch_client,
                                           index=OPENSEARCH_INDEX_GITHUB_ISSUES,
                                           query=query_body,
                                           request_timeout=120,
                                           )

    # 不要在dag or task里面 创建index 会有并发异常！！！
    # if not opensearch_client.indices.exists("github_issues"):
    #     opensearch_client.indices.create("github_issues")

    # 由于需要初始化幂等要求，在重新初始化前删除对应owner/repo 指定的 issues_timeline 记录的所有数据
    del_result = opensearch_client.delete_by_query(index=OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE,
                                                   body=query_body,
                                                   request_timeout=120,
                                                   )

    logger.info(f"DELETE github issues_timeline result: {del_result}")

    get_timeline_fails_results = list()

    def handle_batch(num_batch):
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for number in num_batch:
                futures.append(
                    executor.submit(do_get_github_timeline,
                                    opensearch_client, token_proxy_accommodator, owner, repo, number))
            for f in as_completed(futures):
                status_code, msg = f.result()
                if status_code != 200:
                    logger.error(f"Failed to init timeline: {msg}")
                    get_timeline_fails_results.append(status_code)

    batch_size = 10  # 10 timelines data in a batch
    num_handled = 0
    batch = []

    # Fetch all the issue numbers from scan_result, this is not necessary programmatically, while the scan result is an
    # iterator which hide everything underneath, if we loop on the iter and do some time-consuming job(like fetching
    # timeline data), the search context might be cleaned up and bring fails on search, it's discussed by the thread:
    # https://discuss.elastic.co/t/what-does-no-search-context-found-for-id-mean/103316/9
    issue_nums = [issue['_source']['raw_data']['number'] for issue in scan_results]
    for issue_num in issue_nums:
        batch.append(issue_num)
        if len(batch) >= batch_size:
            handle_batch(batch)
            num_handled += len(batch)
            batch.clear()

            logger.info(f'{num_handled} timeline data processed')

    if batch:
        handle_batch(batch)
        num_handled += len(batch)
        batch.clear()
        logger.info(f'Finally, {num_handled} timeline data processed')

    # TODO Should it be thrown as exception?
    #  Or just remain the fails and the missing data will be covered in the daily sync process?
    if get_timeline_fails_results:
        logger.error(f'{len(get_timeline_fails_results)} timeline data items failed to init')


def do_get_github_timeline(opensearch_client, token_proxy_accommodator, owner, repo, number):
    req_session = requests.Session()
    github_api = GithubAPI()
    opensearch_api = OpensearchAPI()

    page = 1
    while True:
        time.sleep(random.uniform(GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX))
        one_page_github_issues_timeline = None
        try:
            req = github_api.get_github_issues_timeline(
                req_session, token_proxy_accommodator, owner, repo, number, page)
            one_page_github_issues_timeline = req.json()
        except GithubResourceNotFoundError as e:
            logger.error(
                f"Failed initing github timeline, {owner}/{repo}, issues_number:{number}, now_page:{page}, Target timeline info does not exist: {e}, end")
            # return 403, e

        if not one_page_github_issues_timeline:
            logger.info(f"success init github timeline, {owner}/{repo}, issues_number:{number}, page_count:{page}, end")
            return 200, f"success init github timeline, {owner}/{repo}, issues_number:{number}, page_count:{page}, end"

        opensearch_api.bulk_github_issues_timeline(opensearch_client=opensearch_client,
                                                   issues_timelines=one_page_github_issues_timeline,
                                                   owner=owner, repo=repo, number=number, if_sync=0)

        logger.info(f"success get github timeline, {owner}/{repo}, issues_number:{number}, page_index:{page}, end")
        page += 1
