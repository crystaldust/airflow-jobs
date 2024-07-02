# company_email_domain_map

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO, COMPANY_EMAIL_DOMAIN_MAP
from oss_know.libs.middle_tables.company_mapping import arrange_company_maps_into_letter_groups, login_company_map_ck
from oss_know.libs.util.clickhouse_driver import CKServer

clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
ck_client = CKServer(host=clickhouse_conn_info.get("HOST"),
                     port=clickhouse_conn_info.get("PORT"),
                     user=clickhouse_conn_info.get("USER"),
                     password=clickhouse_conn_info.get("PASSWD"),
                     database=clickhouse_conn_info.get("DATABASE"),
                     settings={
                         "max_execution_time": 1000000,
                     },
                     kwargs={
                         "connect_timeout": 200,
                         "send_receive_timeout": 6000,
                         "sync_request_timeout": 100,
                     })

with DAG(dag_id='dag_company_mapping',  # schedule_interval='*/5 * * * *',
         schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False,
         tags=['github', 'middle table'], ) as dag:
    def do_login_company_mapping(company_mappings):
        login_company_map_ck(company_mappings, ck_client)
        # TODO Create email -> company mapping


    company_email_domain_map = Variable.get(COMPANY_EMAIL_DOMAIN_MAP, deserialize_json=True)
    groups = arrange_company_maps_into_letter_groups(company_email_domain_map)

    for letter, company_domain_maps in groups.items():
        op_sync_github_commits_from_clickhouse_group = PythonOperator(
            task_id=f'op_login_company_map_group_{letter}',
            python_callable=do_login_company_mapping,
            trigger_rule='all_done',
            op_kwargs={
                "company_mappings": company_domain_maps
            }
        )
