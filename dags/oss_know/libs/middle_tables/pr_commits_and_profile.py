import datetime
import random
import time

from opensearchpy import helpers, OpenSearch

from oss_know.libs.github.graphql import GithubCrawl
from oss_know.libs.util.clickhouse_driver import CKServer
from oss_know.libs.util.log import logger


def get_timestamp():
    current_time = datetime.datetime.now()
    current_timestamp = current_time.timestamp() * 1000
    return current_time, int(current_timestamp)


def login_data_exists(os_client, login):
    results = os_client.search(index='graphql_user_info', body={
        "query": {
            "match": {
                "github_login.keyword": login
            }
        }

    })
    if results['hits']['total']['value'] > 0:
        return True
    return False


def parse_user_info(user_info_container, raw_data, login):
    # user_contributionsCollection = raw_data['data']['user']['contributionsCollection']
    if raw_data.get('errors'):
        return
    if not raw_data['data']['user']:
        return
    current_time, current_timestamp = get_timestamp()
    logger.info(f"当前时间：{current_time}")
    user_info_container.append(
        {
            "_index": "graphql_user_info",
            "_source": {
                "github_login": login,
                "crawl_at": current_time,
                "crawl_timestamp": current_timestamp,
                "raw_data": {
                    # "contributionsCollection" :user_contributionsCollection,
                    "id": raw_data['data']['user']['id'],
                    "company": raw_data['data']['user']['company'],
                    "location": raw_data['data']['user']['location'],
                    "name": raw_data['data']['user']['name'],
                    "bio": raw_data['data']['user']['bio'],
                    "email": raw_data['data']['user']['email']
                }
            }
        }
    )


def parse_pr_commit_files(container, raw_data, login, g):
    if raw_data.get('errors'):
        logger.error(f"{login},errors {raw_data.get('errors')}---------------------------------------------")
        return '', False, 0
    end_cursor = raw_data['data']['user']['pullRequests']['pageInfo']['endCursor']
    # try:
    #     endCursor = raw_data['data']['user']['pullRequests']['pageInfo']['endCursor']
    # except Exception:
    #     print(raw_data)
    #     raise Exception
    has_next_page = raw_data['data']['user']['pullRequests']['pageInfo']['hasNextPage']
    current_time, current_timestamp = get_timestamp()
    page_info = raw_data['data']['user']['pullRequests']['pageInfo']
    total_count = raw_data['data']['user']['pullRequests']['totalCount']
    for data in raw_data['data']['user']['pullRequests']['nodes']:
        # todo 有pr commit可能超过 100（数量少） 但一次只能获取前100 未能全部获取 通过游标进行全部获取再扔进 data里
        # 如果为未被merge且closed 那么直接跳过获取
        merged = data['merged']  # TODO merged is not used

        owner = data['repository']['owner']['login']
        repo = data['repository']['name']
        number = data['url'].split('/')[-1]
        commits_has_next_page = data['commits']['pageInfo']['hasNextPage']
        commits_cursor = data['commits']['pageInfo']['endCursor']
        files_has_next_page = data['files']['pageInfo']['hasNextPage']
        files_end_cursor = data['files']['pageInfo']['endCursor']
        count = 0
        while commits_has_next_page or files_has_next_page:
            raw_data, status_code = g.graphql_post(login, issues_page_info_end_cursor=end_cursor,
                                                   files_page_info_end_cursor=files_end_cursor,
                                                   commits_page_info_end_cursor=commits_cursor,
                                                   owner=owner,
                                                   repo=repo,
                                                   number=number
                                                   )
            commits_has_next_page = False
            files_has_next_page = False
            count += 1

            if raw_data['data']['repository']['pullRequest'].get('commits'):
                data['commits']['nodes'] += raw_data['data']['repository']['pullRequest']['commits']['nodes']
                commits_has_next_page = raw_data['data']['repository']['pullRequest']['commits']['pageInfo'][
                    'hasNextPage']
                if not commits_has_next_page:
                    commits_cursor = ''
                else:
                    commits_cursor = raw_data['data']['repository']['pullRequest']['commits']['pageInfo']['endCursor']
            if raw_data['data']['repository']['pullRequest'].get('files'):
                data['files']['nodes'] += raw_data['data']['repository']['pullRequest']['files']['nodes']
                files_has_next_page = raw_data['data']['repository']['pullRequest']['files']['pageInfo'][
                    'hasNextPage']

                files_end_cursor = raw_data['data']['repository']['pullRequest']['files']['pageInfo']['endCursor']

        container.append(
            {
                "_index": "user_pr_commit_files",
                "_source": {
                    "github_login": login,
                    "crawl_at": current_time,
                    "crawl_timestamp": current_timestamp,
                    "page_info": page_info,
                    "prTotalCount": total_count,
                    "raw_data": data}
            }
        )
    return end_cursor, has_next_page, total_count


def get_pr_commit_files_and_profile_from_logins(os_client, logins, proxy_accommodator):
    for login in logins:
        if login_data_exists(os_client, login):
            time.sleep(2)
            logger.info(f"{login}数据已存在跳过获取")
            continue

        logger.info(f"开始获取:{login} pr数据")
        g = GithubCrawl(proxy_accommodator)
        user_info = []
        pr_commit_files_data = []
        raw_data = None

        try:
            graphql_data, _ = g.graphql_post(login)
            errors = graphql_data.get('errors')
            if errors:
                if len(errors) == 1:
                    if errors[0]['type'] == 'NOT_FOUND':
                        logger.error(f"用户{login}不存在直接跳到下一个用户")
                        continue
            raw_data = graphql_data
        except Exception as e:
            logger.error(f"{login} post 错误: {e}")
            # raise e
            continue
        if not raw_data:
            continue

        end_cursor, has_next_page, total_count = parse_pr_commit_files(pr_commit_files_data, raw_data, login, g)
        parse_user_info(user_info, raw_data, login)

        # 游标迭代器
        while has_next_page:
            try:
                raw_data, status_code = g.graphql_post(login, issues_page_info_end_cursor=end_cursor)
            except Exception as e:
                # TODO Capture the exception types
                logger.error(f"{login} post 错误 {e}")
                # raise Exception
                # raw_data, status_code = g.graphql_post(login, issues_page_info_end_cursor=end_cursor)
                logger.warning(f"重复多次依旧失败 直接跳过开发者")
                break
            if not raw_data:
                break
            end_cursor, has_next_page, total_count = parse_pr_commit_files(pr_commit_files_data, raw_data, login, g)

        if len(pr_commit_files_data) != total_count:
            logger.warning('数据未完全获取 跳过开发者')
            continue
        # 插入数据
        if pr_commit_files_data:
            success, failed = helpers.bulk(os_client, pr_commit_files_data)
            logger.info(f"插入用户 {login} pr数据 :{success},失败：{failed}")
            if total_count != success:
                logger.error(
                    f"用户 {login} pr数据未获取完全  {total_count}-->{success}, raw_data {raw_data}  ====== {login}")
        if user_info:
            success, failed = helpers.bulk(os_client, user_info)
            logger.info(f"插入用户 {login} profile 数据:{success},失败：{failed} ")
        time.sleep(random.randint(1, 3))


# From graphql user info to company contributor profile(ck)
def insert_company_contributor_profile(os_client: OpenSearch, ck_client: CKServer):
    max_crawl_timestamp = ck_client.execute_no_params("""select max(crawl_timestamp) as timestamp
    from company_contributor_profile""")[0][0]

    results = helpers.scan(os_client,
                           query={
                               "query": {
                                   "range": {
                                       "crawl_timestamp": {
                                           "gt": max_crawl_timestamp
                                       }
                                   }
                               }
                           },
                           index='graphql_user_info')  # TODO v4????
    bulk_data = []
    for result in results:
        result['_source']['node_id'] = '' if not result['_source']['raw_data']['id'] \
            else result['_source']['raw_data']['id']
        result['_source']['company'] = '' if not result['_source']['raw_data']['company'] \
            else result['_source']['raw_data']['company']
        result['_source']['location'] = '' if not result['_source']['raw_data']['location'] \
            else result['_source']['raw_data']['location']
        result['_source']['name'] = '' if not result['_source']['raw_data']['name'] \
            else result['_source']['raw_data']['name']
        result['_source']['bio'] = '' if not result['_source']['raw_data']['bio'] \
            else result['_source']['raw_data']['bio']
        result['_source']['email'] = '' if not result['_source']['raw_data']['email'] \
            else result['_source']['raw_data']['email']
        result['_source']['data_insert_at'] = int(time.time() * 1000)
        result['_source']['crawl_timestamp'] = int(result['_source']['crawl_timestamp'])

        bulk_data.append(result['_source'])

    success = ck_client.execute("insert into table company_contributor_profile values", bulk_data)


# From user pr commit files(os) to company contributor pr(ck)
def insert_company_contributor_pr(os_client: OpenSearch, ck_client: CKServer):
    max_crawl_timestamp = ck_client.execute_no_params("""select max(crawl_timestamp) as timestamp
    from company_contributor_pr""")[0][0]
    results = helpers.scan(os_client,
                           query={
                               "query": {
                                   "range": {
                                       "crawl_timestamp": {
                                           "gt": max_crawl_timestamp
                                       }
                                   }
                               }
                           },
                           index="user_pr_commit_files",
                           )

    bulk_data = []
    batch_size = 10000
    num_inserted = 0
    for result in results:
        result['_source']['data_insert_at'] = int(time.time())
        result['_source']['id'] = result['_source']['raw_data']['id']

        result['_source']['owner'] = result['_source']['raw_data']['repository']['owner']['login']
        result['_source']['repo'] = result['_source']['raw_data']['repository']['name']
        result['_source']['isFork'] = 1 if result['_source']['raw_data']['repository']['isFork'] else 0
        result['_source']['forkCount'] = result['_source']['raw_data']['repository']['forkCount']
        result['_source']['stargazerCount'] = result['_source']['raw_data']['repository']['stargazerCount']

        result['_source']['title'] = result['_source']['raw_data']['title']
        result['_source']['createdAt'] = result['_source']['raw_data']['createdAt']

        result['_source']['url'] = result['_source']['raw_data']['url']
        result['_source']['merged'] = 1 if result['_source']['raw_data']['merged'] else 0

        result['_source']['mergedAt'] = '' if not result['_source']['raw_data']['mergedAt'] else \
            result['_source']['raw_data']['mergedAt']
        result['_source']['closedAt'] = '' if not result['_source']['raw_data']['closedAt'] else \
            result['_source']['raw_data']['closedAt']

        result['_source']['additions'] = result['_source']['raw_data']['additions']
        result['_source']['deletions'] = result['_source']['raw_data']['deletions']
        result['_source']['changedFiles'] = result['_source']['raw_data']['changedFiles']

        result['_source']['crawl_timestamp'] = int(result['_source']['crawl_timestamp'])

        result['_source']['files.path'] = []
        result['_source']['files.additions'] = []
        result['_source']['files.deletions'] = []
        result['_source']['files.changeType'] = []
        result['_source']['commits.message'] = []
        result['_source']['commits.author_name'] = []
        result['_source']['commits.author_date'] = []
        result['_source']['commits.author_email'] = []
        result['_source']['commits.committer_name'] = []
        result['_source']['commits.committer_date'] = []
        result['_source']['commits.committer_email'] = []
        result['_source']['commits.authoredByCommitter'] = []
        result['_source']['commits_totalCount'] = 0
        result['_source']['commits.id'] = []
        result['_source']['commits.oid'] = []
        result['_source']['commits.author_github_node_id'] = []
        result['_source']['commits.author_github_name'] = []
        result['_source']['commits.author_github_login'] = []
        result['_source']['commits.author_github_company'] = []
        result['_source']['commits.author_github_location'] = []

        result['_source']['commits.committer_github_node_id'] = []
        result['_source']['commits.committer_github_name'] = []
        result['_source']['commits.committer_github_login'] = []
        result['_source']['commits.committer_github_company'] = []
        result['_source']['commits.committer_github_location'] = []

        if result['_source']['raw_data']['files']:
            files = result['_source']['raw_data']['files']['nodes']
            for file in files:
                result['_source']['files.path'].append(file['path'])
                result['_source']['files.additions'].append(file['additions'])
                result['_source']['files.deletions'].append(file['deletions'])
                result['_source']['files.changeType'].append(file['changeType'])

        if result['_source']['raw_data']['commits']:
            commits = result['_source']['raw_data']['commits']['nodes']
            result['_source']['commits_totalCount'] = result['_source']['raw_data']['commits']['totalCount']

            for commit in commits:
                result['_source']['commits.id'].append(commit['commit']['id'])
                result['_source']['commits.oid'].append(commit['commit']['oid'])

                result['_source']['commits.message'].append(commit['commit']['message'])
                result['_source']['commits.author_name'].append(commit['commit']['author']['name'])
                result['_source']['commits.author_date'].append(commit['commit']['author']['date'])
                result['_source']['commits.author_email'].append(commit['commit']['author']['email'])
                result['_source']['commits.committer_name'].append(commit['commit']['committer']['name'])
                result['_source']['commits.committer_date'].append(commit['commit']['committer']['date'])
                result['_source']['commits.committer_email'].append(commit['commit']['committer']['email'])
                result['_source']['commits.authoredByCommitter'].append(
                    1 if commit['commit']['authoredByCommitter'] else 0)

                if commit['commit']['author']['user']:
                    result['_source']['commits.author_github_node_id'].append(commit['commit']['author']['user']['id'])
                    result['_source']['commits.author_github_name'].append(
                        '' if not commit['commit']['author']['user']['name']
                        else commit['commit']['author']['user']['name'])
                    result['_source']['commits.author_github_login'].append(
                        '' if not commit['commit']['author']['user']['login']
                        else commit['commit']['author']['user']['login'])
                    result['_source']['commits.author_github_company'].append(
                        '' if not commit['commit']['author']['user']['company']
                        else commit['commit']['author']['user']['company'])
                    result['_source']['commits.author_github_location'].append(
                        '' if not commit['commit']['author']['user']['location']
                        else commit['commit']['author']['user']['location'])
                else:

                    result['_source']['commits.author_github_node_id'].append('')
                    result['_source']['commits.author_github_name'].append('')
                    result['_source']['commits.author_github_login'].append('')
                    result['_source']['commits.author_github_company'].append('')
                    result['_source']['commits.author_github_location'].append('')

                if commit['commit']['committer']['user']:
                    result['_source']['commits.committer_github_node_id'].append(
                        commit['commit']['committer']['user']['id'])
                    result['_source']['commits.committer_github_name'].append(
                        '' if not commit['commit']['committer']['user']['name']
                        else commit['commit']['committer']['user']['name'])
                    result['_source']['commits.committer_github_login'].append(
                        '' if not commit['commit']['committer']['user']['login']
                        else commit['commit']['committer']['user']['login'])
                    result['_source']['commits.committer_github_company'].append(
                        '' if not commit['commit']['committer']['user']['company']
                        else commit['commit']['committer']['user']['company'])
                    result['_source']['commits.committer_github_location'].append(
                        '' if not commit['commit']['committer']['user']['location']
                        else commit['commit']['committer']['user']['location'])
                else:
                    result['_source']['commits.committer_github_node_id'].append('')
                    result['_source']['commits.committer_github_name'].append('')
                    result['_source']['commits.committer_github_login'].append('')
                    result['_source']['commits.committer_github_company'].append('')
                    result['_source']['commits.committer_github_location'].append('')

        bulk_data.append(result['_source'])

        """
        isFork Int64,
        forkCount Int64,
        stargazerCount Int64,
        `files.path`  Array(String),
        `files.additions` Array(Int64),
        `files.deletions` Array(Int64),
        `files.changeType` Array(String),
        `commits.message` Array(String),
        `commits.author_name` Array(String),
        `commits.author_date` Array(String),
        `commits.author_email` Array(String),
        `commits.committer_name` Array(String),
        `commits.committer_date` Array(String),
        `commits.committer_email` Array(String),
        `commits.authoredByCommitter` Array(Int64),
        `commits.author_github_node_id` Array(String),
        `commits.author_github_name` Array(String),
        `commits.author_github_login` Array(String),
        `commits.author_github_company` Array(String),
        `commits.author_github_location` Array(String),
        `commits.committer_github_node_id` Array(String),
        `commits.committer_github_name` Array(String),
        `commits.committer_github_login` Array(String),
        `commits.committer_github_company` Array(String),
        `commits.committer_github_location` Array(String),
        `commits.id` Array(String),
        `commits.oid` Array(String)
        ``
        """

        if len(bulk_data) % batch_size == 0:
            num_success = ck_client.execute("insert into table company_contributor_pr values", bulk_data)
            num_inserted += num_success
            logger.info(f'{num_inserted} records inserted company_contributor pr')
            bulk_data.clear()

    if bulk_data:
        num_success = ck_client.execute("insert into table company_contributor_pr values", bulk_data)
        num_inserted += num_success
        logger.info(f'{num_inserted} records inserted company_contributor pr')
        bulk_data.clear()
