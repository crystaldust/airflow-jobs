from oss_know.libs.maillist.subscription import sync_imap_subscription

maillist_subscription_accounts_str = '''
[
    {
        "email_address": "your_email_address",
        "password": "your_password",
        "imap_host": "imap.your_mail_host.com",
        "imap_port": 993
    }
]
'''


opensearch_conn_data = '''
{
  "HOST": "192.168.8.10",
  "PASSWD": "password",
  "PORT": "9200",
  "USER": "user"
}
'''
import json
maillist_subscription_accounts = json.loads(maillist_subscription_accounts_str)
opensearch_conn_info = json.loads(opensearch_conn_data)


import perceval
# print(perceval.backends.core.pipermail.logger.level)
# print(perceval.backends.core.pipermail.logger.getEffectiveLevel())

# import logging
# logging.basicConfig(level="DEBUG")

# exit(0)

for account in maillist_subscription_accounts:
    email_address = account['email_address']
    password = account['password']
    imap_host = account['imap_host']
    imap_port = account['imap_port']
    sync_imap_subscription(opensearch_conn_info, email_address, password, imap_host, imap_port, folder='etcd')
