import json

from oss_know.libs.maillist.archive import sync_archive

# v0.0.1 It is a mailing list DAG


mail_lists = json.loads('''
[
  {
    "project_name": "linux",
    "archive_type": "mbox",
    "list_name": "kernel",
    "url_prefix": "https://lkml.org/",
    "mbox_path": "./lkml-sample-mboxes"
  }
]
''')

opensearch_conn_info = json.loads('''
{
    "HOST": "192.168.8.21",
    "PASSWD": "admin",
    "PORT": "19201",
    "USER": "admin"
}
''')
from glob import glob

mbox_glob = glob('/home/lance/Development/threads_mbox_gzips/*.mbox')

from concurrent.futures import ThreadPoolExecutor

executor = ThreadPoolExecutor(20)

for file_path in mbox_glob:
    print(file_path)
    # if '00a001c75f0c$47f19750$4b00a8c0' not in file_path:
    #     continue
    mail_list_info = {
        "project_name": "linux",
        "archive_type": "mbox",
        "list_name": "kernel",
        "url_prefix": "https://lkml.org/",
        "mbox_path": file_path,
        "thread_id": file_path.split("/")[-1].lstrip("./").rstrip(".mbox")
    }
    executor.submit(sync_archive, opensearch_conn_info, **mail_list_info)
    # sync_archive(opensearch_conn_info, **mail_list_info)
executor.shutdown()
