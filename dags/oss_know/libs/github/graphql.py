import time

import requests
from tenacity import retry, stop_after_attempt

from oss_know.libs.util.base import wrap_github_request_session
from oss_know.libs.util.proxy import GithubTokenProxyAccommodator
from oss_know.libs.util.log import logger


class GithubCrawl:
    url = "https://api.github.com/graphql"
    RETRY_MAX_ATTEMPTS = 6
    count = 0

    def __init__(self, proxy_accommodator: GithubTokenProxyAccommodator):
        self.session = requests.session()
        self.proxy_tokens_accommodator = proxy_accommodator

    def get_issue_graphql_query_body(self, github_login, issues_page_info_end_cursor, issue_type='pullRequests'):
        if issues_page_info_end_cursor:
            after_cursor = f', after:"{issues_page_info_end_cursor}"'
        else:
            after_cursor = ''

        query = ''
        if issue_type == 'pullRequests':
            # 加字段 ，每个commit 加减行
            query = f"""

            {{
  user(login: "{github_login}"){{
    id
    company
    location
    name
    bio
    email
    pullRequests(first: 100{after_cursor}) {{
      totalCount
      pageInfo {{
            endCursor
            startCursor
            hasNextPage
            hasPreviousPage
          }}
      nodes {{
        id
        repository{{
          owner{{
            login
          }}
          name
          isFork
          forkCount
          stargazerCount
        }}

        title
        createdAt
        url
        merged
        mergedAt
        closedAt

        additions
        deletions
        changedFiles
        commits(first: 100){{
        totalCount
          pageInfo {{
            endCursor
            startCursor
            hasNextPage
            hasPreviousPage
          }}
          nodes{{
            commit{{
              additions
              deletions
              message
              id
              oid
              parents{{
                totalCount
              }}
              author{{
                name
                date
                email
                user{{
                  id
                  name
                  login
                  company
                  location
                }}
              }}
              committer{{
                name
                date
                email
                user{{
                  id
                  name
                  login
                  company
                  location
                }}
              }}
              authoredByCommitter
            }}
					}}
        }}
        files(first: 100){{
        totalCount
          pageInfo {{
            endCursor
            startCursor
            hasNextPage
            hasPreviousPage
          }}
          nodes{{
            path
            additions
            deletions
            changeType
          }}
        }}

      }}
    }}
  }}

}}
            """

        return query

    def get_repo_contributors(self, owner_repo):
        """
        获取仓库的所有贡献者
        :param owner_repo:
        :return:
        """
        contributors_list = []
        url = f'https://api.github.com/repos/{owner_repo}/contributors'
        print(f'开始获取数据：{url}')

        headers = {'Authorization': 'token %s' % self.api_token,
                   "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                   "accept": "application/vnd.github+json"}
        page = 1
        while True:
            params = {"page": page, "per_page": 100}
            req = requests.get(url, headers=headers, params=params
                               # , proxies=self.proxy
                               )
            req_json = req.json()

            if not req_json or (isinstance(req_json, dict) and req_json.get('message') == 'Not Found'):
                break

            contributors_list.append(req_json)
            page += 1

        return contributors_list

    # 将commit 和 files迭代全
    def get_commit_files(self, commits_page_info_end_cursor, files_page_info_end_cursor, owner, repo, number):
        """
        获取commit 和 files
        :param commits_page_info_end_cursor: commit 最后一条数据
        :param files_page_info_end_cursor: files的最后一条数据
        :param owner:
        :param repo:
        :param number:
        :return:
        """
        commits_query = ''
        files_query = ''
        if commits_page_info_end_cursor:
            commits_query = f'''
            commits(first: 100, after:"{commits_page_info_end_cursor}") {{
        totalCount
        pageInfo {{
          endCursor
          startCursor
          hasNextPage
          hasPreviousPage
        }}
        nodes {{
          commit {{
            id
            oid
            parents{{
                totalCount
              }}
            message
            deletions
            additions
            authoredDate
            committedDate
            author {{
              user {{
                id
                name
                login
                company
                location
              }}
              name
              date
              email
            }}
            committer {{
              name
              date
              email
              user {{
                id
                name
                login
                company
                location
              }}
            }}
            authoredByCommitter
          }}
        }}
      }}

            '''

        if files_page_info_end_cursor:
            files_query = f'''
            files(first: 100, after:"{files_page_info_end_cursor}") {{
        totalCount
        pageInfo {{
          endCursor
          startCursor
          hasNextPage
          hasPreviousPage
        }}
        nodes {{
          path
          additions
          deletions
          changeType
        }}
      }}

            '''

        if commits_query and files_query:

            query = f'''
            {{
            repository(owner: "{owner}", name: "{repo}") {{
                pullRequest(number: {number}) {{
                {commits_query}
                {files_query}


    }}
  }}
}}      

            '''
        elif commits_query and not files_query:

            query = f'''
                        {{
                        repository(owner: "{owner}", name: "{repo}") {{
                            pullRequest(number: {number}) {{
                            {commits_query}


                }}
              }}
            }}      

                        '''
        elif not commits_query and files_query:
            query = f'''
                                    {{
                                    repository(owner: "{owner}", name: "{repo}") {{
                                        pullRequest(number: {number}) {{
                                        {files_query}


                            }}
                          }}
                        }}      

                                    '''
        return query

    def if_sha_exist(self, sha, index):
        osh = OpensearchHook()
        results = osh.search(index=index, body={
            "query": {
                "match": {
                    "sha.keyword": sha
                }
            }

        })
        if results['hits']['total']['value'] > 0:
            return True
        osh.client.close()
        return False

    def get_author_info_by_sha(self, owner, repo, sha):
        graphql_ = f"""
        query {{
  repository(owner: "{owner}", name: "{repo}") {{
    object(expression: "{sha}") {{
      ... on Commit {{
        additions
        deletions
        message
        parents(first: 5){{
         totalCount
          nodes{{
            oid
          }}
        }}
        author {{
          user{{
            login
            name
            company
            location
            bio
            email
          }}
          name
          email
          date
        }}
        committer {{
          user{{
            login
            name
            company
            location
            bio
            email
          }}
          name
          email
          date
        }}

        associatedPullRequests(first: 10) {{
          nodes {{
            author{{
              login
            }}
            merged
            mergedAt
            closed
            closedAt
            createdAt
            body
            title
            number
            url
          }}
        }}
      }}
    }}
  }}
}}


        """
        return graphql_

    def _send_request(self, proxy, headers, graphql_query):
        return self.session.post(self.url, headers=headers, proxies=proxy, json={"query": graphql_query})

    def _handle_message(self, message, token):
        if message:
            if message.startswith('You have exceeded a secondary rate limit.'):
                print(f"请求速率太快，二次速率限制 token：{token}")
                time.sleep(5)
                raise Exception('请求速率太快，二次速率限制')

    def sha_to_github_login_to_ck(self):
        pass

    def _handle_errors(self, errors, token):
        # print('进入handle_errors---------')
        if errors:
            # print('进入handle_errors---------')
            if len(errors) >= 1:
                if errors[0]['message'].startswith('Your token has'):
                    print(f"token__权限不够 ---- 》{token} lalala")
                    return 404
                if errors[0]['message'].startswith('Could not resolve to a Repository with the name'):
                    print(errors[0]['message'])
                    return 404
            # todo 给一个404

        return 200

    @retry(retry=stop_after_attempt(6))
    def graphql_post_email(self, graphql_):
        time.sleep(0.5)
        token, proxy = self.proxy_tokens_accommodator.next()
        github_headers = {
            # 'Authorization': 'token %s' % token,
            'Authorization': f'token {token}',
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "accept": "application/vnd.github+json"}

        r = self.session.post(self.url, headers=github_headers, proxies=proxy, json={"query": graphql_})
        if r.status_code >= 500:
            self.count += 1
            time.sleep(2)
            print(f' :重试次数 {self.count} status_code:{r.status_code}')
        errors = r.json().get('errors')
        message = r.json().get('message')
        self._handle_message(message, token)
        status_code = self._handle_errors(errors, token)

        return r.json(), status_code

    @retry(stop=stop_after_attempt(6))
    def graphql_post(self, github_login, issues_page_info_end_cursor='', commits_page_info_end_cursor='',
                     files_page_info_end_cursor='', owner='', repo='', number=''):

        if commits_page_info_end_cursor == '' and files_page_info_end_cursor == '':
            query = self.get_issue_graphql_query_body(github_login, issues_page_info_end_cursor)
        else:
            query = self.get_commit_files(commits_page_info_end_cursor, files_page_info_end_cursor, owner, repo, number)

        self.session.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            'accept': 'application/vnd.github+json'
        }
        wrap_github_request_session(self.session, self.url, self.proxy_tokens_accommodator)
        self.session.proxies = {}  # debugging

        r = self.session.post(self.url, json={"query": query})

        if r.status_code >= 500:
            # 502是json
            # 504是status code
            self.count += 1
            time.sleep(2)
            logger.info(f'{github_login} :重试次数 {self.count} status_code:{r.status_code}')
            logger.debug(r.text)
            # print(f'r是什么{r}')
            # print('r.text', r.text)
            # print(f'token是什么：{token}')
            # print(f'status_code:{r.status_code}')

            raise Exception(r.status_code)

        res_json = r.json()
        # errors = res_json.get('errors')
        # message = res_json.get('message')
        # self._handle_message(message, token)
        # status_code = self._handle_errors(errors, token)

        return res_json, r.status_code
