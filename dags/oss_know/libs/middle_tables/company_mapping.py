# Get mappings of (email -> company), (login -> company)
import time
from string import ascii_lowercase

from oss_know.libs.util.log import logger


def arrange_company_maps_into_letter_groups(company_maps):
    groups = {'other': {}}
    for letter in ascii_lowercase:
        groups[letter] = {}

    # company_maps sample:
    # {
    #     "nvidia": {
    #         "company_list": [
    #             "nvidia"
    #         ],
    #         "domain_list": [
    #             "nvidia.com"
    #         ]
    #     }
    # }
    for company, info in company_maps.items():
        capital_letter = company[0].lower()
        key = capital_letter if capital_letter in groups else 'other'
        groups[key][company] = info

    return groups


def login_company_map_ck(company_mappings, ck_client):
    for company, info in company_mappings.items():
        company_list = info['company_list']
        company_email_domain_list = info['domain_list']
        sql_ = login_company_map_sql(company, company_list, company_email_domain_list)
        print(sql_)
        results = ck_client.execute_no_params(sql_)

        bulk_data = []
        bulk_size = 10000
        num_inserted = 0
        for result in results:
            login = result[0]
            # company = result[1]
            insert_at = int(time.time() * 1000)
            bulk_data.append({
                "github_login": login,
                "company": company,
                "insert_at": insert_at
            })
            if len(bulk_data) >= bulk_size:
                ck_client.execute('insert into github_login_company_map values', bulk_data)
                num_inserted += len(bulk_data)
                logger.info(f'{num_inserted} record inserted for login company map of {company}')
                bulk_data.clear()
        if bulk_data:
            ck_client.execute('insert into github_login_company_map values', bulk_data)
            num_inserted += len(bulk_data)
            logger.info(f'{num_inserted} record inserted for login company map of {company}')


def login_company_map_sql(company_name, company_list, company_email_domain_list):
    # TODO How to construct these 2 tables?
    company_contributor_profile_v3 = 'company_contributor_profile_v3'
    company_contributor_pr_v3 = 'company_contributor_pr_v3'

    email_domain_str = ' or '.join([f"lower(email) like '%{d}%'" for d in company_email_domain_list])
    company_str = ' or '.join([f"lower(company) like '%{c}%'" for c in company_list])
    final_company_str = ' or '.join([f"lower(final_company_inferred_from_company)='{c}'" for c in company_list])

    return f"""
    select author__login, '{company_name}' as company
    from (
             -- github_commit 通过邮箱找company
             select author__login
             from (select author__login
                   from (select author__login,commit__author__email as email from github_commits)
                   where author__login != ''
                     and author__login not like '%[bot]%'
                     and ({email_domain_str})
                   group by author__login

                   union all

                   select committer__login
                   from  (select committer__login,commit__committer__email as email from github_commits)
                   where committer__login != ''
                     and committer__login not like '%[bot]%'
                     and ({email_domain_str})
                   group by committer__login)
             group by author__login

             union all
             -- 在github profile 里找
             select github_login
             from (select a.*, b.final_company_inferred_from_company
                   from (select github_login, company from {company_contributor_profile_v3}) as a global
                            left join (select company, final_company_inferred_from_company
                                       from github_profile
                                       where company != ''
                                         and final_company_inferred_from_company != ''
                                       group by company, final_company_inferred_from_company) as b
                                      on a.company = b.company
                   where ({final_company_str}
                       or {company_str})
                     and github_login != '')

             group by github_login

             union all
             -- 从人出发查看人的 pr commit 关联的邮箱和company 是否为company
             select github_login
             from (select commits.author_email            as email,
                          commits.author_github_login     as github_login,
                          `commits.author_github_company` as company
                   from {company_contributor_pr_v3} array join commits.author_email, commits.author_github_login, `commits.author_github_company`
                   where ( {email_domain_str}
                       or {company_str})
                     and github_login != '')
             group by github_login

             union all
             -- 也算从profile出发
             select github_login
             from {company_contributor_profile_v3}
             where {company_str}
             group by github_login)
    where author__login != ''
    group by author__login
    """
