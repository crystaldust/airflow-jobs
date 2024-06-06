import random
import time
from datetime import datetime, timezone

import requests
from opensearchpy import OpenSearch

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS
from oss_know.libs.base_dict.options import GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX
from oss_know.libs.util.github_api import GithubAPI
from oss_know.libs.util.log import logger
from oss_know.libs.util.opensearch_api import OpensearchAPI


class SyncGithubPullRequestsException(Exception):
    def __init__(self, message, status):
        super().__init__(message, status)
        self.message = message
        self.status = status


def sync_github_pull_requests(opensearch_conn_info,
                              owner,
                              repo,
                              token_proxy_accommodator
                              ):
    logger.info("start Function to be renamed to sync_github_pull_requests")
    now_time = datetime.utcnow()
    opensearch_client = OpenSearch(
        hosts=[{'host': opensearch_conn_info["HOST"], 'port': opensearch_conn_info["PORT"]}],
        http_compress=True,
        http_auth=(opensearch_conn_info["USER"], opensearch_conn_info["PASSWD"]),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )
    opensearch_api = OpensearchAPI()

    since = None
    pr_checkpoint = opensearch_api.get_checkpoint(opensearch_client,
                                                  OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS, owner, repo)
    if not pr_checkpoint["hits"]["hits"]:
        # Try to get the latest PR date(created_at field) from existing github_pull_requests index
        # And make it the latest checkpoint
        latest_pr_date = get_latest_pr_date_str(opensearch_client, owner, repo)
        if latest_pr_date:
            since = datetime.strptime(latest_pr_date, '%Y-%m-%dT%H:%M:%SZ').astimezone(timezone.utc)
    else:
        github_pull_requests_check = pr_checkpoint["hits"]["hits"][0]["_source"]["github"]["prs"]
        since = datetime.fromtimestamp(github_pull_requests_check["sync_timestamp"], timezone.utc)

    # 生成本次同步的时间范围：同步到今天的 00:00:00
    if not since:
        logger.info(f'Latest PR sync time of {owner}/{repo} not found, sync from scratch')
        since = datetime.fromtimestamp(0, timezone.utc)
    else:
        logger.info(f'Sync github pull requests {owner}/{repo} since：{since}')

    pull_requests_numbers = []
    session = requests.Session()
    github_api = GithubAPI()
    page = 1
    while True:
        # Token sleep
        time.sleep(random.uniform(GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX))
        # The PRs are globally sorted by updated_at desc, so just get all the PRs updated after since, then insert them
        # into Opensearch
        req = github_api.get_github_pull_requests(http_session=session,
                                                  token_proxy_accommodator=token_proxy_accommodator,
                                                  owner=owner,
                                                  repo=repo,
                                                  page=page)

        pr_page = req.json()
        updated_prs = []  # A container to store all updated PRs in the CURRENT page
        # 将since时间之后的插入，遇到since时间之前的数据直接舍弃
        for pr in pr_page:
            # Since the PRs are sorted by updated_at, if we meet the PRs updated before since, we should quit the whole
            # process at this point
            pr_updated_at = datetime.strptime(pr['updated_at'], '%Y-%m-%dT%H:%M:%SZ').astimezone(timezone.utc)
            if pr_updated_at < since:
                if updated_prs:
                    opensearch_api.sync_bulk_github_pull_requests(github_pull_requests=updated_prs,
                                                                  opensearch_client=opensearch_client,
                                                                  owner=owner, repo=repo, if_sync=1)
                return pull_requests_numbers
            updated_prs.append(pr)
            pull_requests_numbers.append(pr["number"])

        # If not updated PRs are added to the container, it should break at the point, since all the following PRs are
        # not updated, and we don't have to handle them.
        if not updated_prs:
            logger.info(f"sync github pull_requests end to break:{owner}/{repo} page_index:{page}")
            break

        # If there are some updated PRs after since, they are stored in the container and should be inserted.
        opensearch_api.sync_bulk_github_pull_requests(github_pull_requests=updated_prs,
                                                      opensearch_client=opensearch_client,
                                                      owner=owner, repo=repo, if_sync=1)
        logger.info(f"success get github pull_requests page:{owner}/{repo} page_index:{page}")
        page += 1

    # 建立 sync 标志
    opensearch_api.set_sync_github_pull_requests_check(opensearch_client=opensearch_client,
                                                       owner=owner, repo=repo, now_time=now_time)

    logger.info(f"pull_requests_list:{pull_requests_numbers}")

    # todo 返回 pull_requests number，获取 pull_requests 独有的更新，进行后续处理
    return pull_requests_numbers


def get_latest_pr_date_str(opensearch_client, owner, repo):
    latest_pr_info = opensearch_client.search(index=OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS,
                                              body={
                                                  "size": 1,
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
                                                  },
                                                  "sort": [
                                                      {
                                                          "raw_data.created_at": {
                                                              "order": "desc"
                                                          }
                                                      }
                                                  ]
                                              }
                                              )
    if not latest_pr_info["hits"]["hits"]:
        return None

    return latest_pr_info["hits"]["hits"][0]["_source"]["raw_data"]["created_at"]
