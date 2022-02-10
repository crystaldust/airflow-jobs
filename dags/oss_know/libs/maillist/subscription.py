from imap_tools import MailBox
import mailbox
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_MAILLISTS
import os
# Debug in local env(with proxy)
import socks
import imaplib
proxy_host = '192.168.8.10'
proxy_port = 7891
socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, proxy_host, proxy_port)
socks.wrapmodule(imaplib)
# Debug in local env(with proxy)

from .archive import sync_archive

def sync_imap_subscription(opensearch_conn_info, email_address, password, imap_host, imap_port=993, folder=None):
    project_name = 'etcd'
    list_name = 'etcd'
    prefix = f'{project_name}/{list_name}'

    opensearch_client = get_opensearch_client(opensearch_conn_info)
    maillist_result = opensearch_client.search(index=f'{OPENSEARCH_INDEX_MAILLISTS}_enriched', body={
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {"search_key.project_name": "etcd"}
                    },
                    {
                        "match": {"search_key.mail_list_name": "etcd"}
                    }
                ]
            }
        },
        "sort": [
            {
                "metadata__timestamp": {"order": "asc"}
            }
        ],
        "_source": "Message-ID",
        "size": 1
    })

    last_inserted_message_id = None
    if maillist_result['hits']['hits']:
        last_inserted_message_id = maillist_result['hits']['hits'][0]['_source']['Message-ID']

    with MailBox(imap_host, port=imap_port).login(email_address, password) as imap_mailbox:
        if folder:
            imap_mailbox.folder.set(folder, readonly=True)

        os.makedirs(f'subscriptions/{prefix}/', exist_ok=True)
        mbox_filepath = f'subscriptions/{prefix}/subscription.mbox'
        mbox = mailbox.mbox(mbox_filepath, create=True)
        mbox.clear()

        # TODO For large amount of emails, use 'limit' parameter for paging
        for msg in imap_mailbox.fetch(reverse=True):
            mbox_message = mailbox.mboxMessage(msg.obj)
            if last_inserted_message_id and last_inserted_message_id == mbox_message['Message-ID']:
                break

            mbox.add(mbox_message)
            print(msg.subject, mbox_message['Message-ID'], last_inserted_message_id)


        # for msg in mbox.items():
        #     print(msg)
        print(len(mbox.items()))

        mbox.close()

        sync_archive(opensearch_conn_info, archive_type='mbox', list_name=list_name, project_name=project_name,
                     clear_exist=False, url_prefix='just_test', mbox_path=mbox_filepath)

