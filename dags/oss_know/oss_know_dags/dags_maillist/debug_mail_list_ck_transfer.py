import json

import pandas as pd

from oss_know.libs.clickhouse import init_ck_transfer_data

# v0.0.1 It is a mailing list DAG


opensearch_conn_info = json.loads('''
{
    "HOST": "192.168.8.60",
    "PASSWD": "admin",
    "PORT": "19201",
    "USER": "admin"
}
''')

clickhouse_conn_info = json.loads('''
{
    "HOST": "192.168.8.60",
    "PORT": "19000",
    "USER": "default",
    "PASSWD": "default",
    "DATABASE": "default"
}
''')

table_templates = None

with open('./ck_table_default_val_tplt.json', 'r') as f:
    table_templates = json.load(f)

template = None
for table_template in table_templates:
    if table_template.get("table_name") == 'maillists':
        template = table_template.get("temp")
        break
df = pd.json_normalize(template)
template = init_ck_transfer_data.parse_data_init(df)
search_key = {
    'project_name': 'busybox',
    'mail_list_name': 'busybox',
}

init_ck_transfer_data.transfer_data_by_repo(clickhouse_conn_info, 'maillists', 'maillists',
                                            opensearch_conn_info, template, search_key, 'maillist_init')
