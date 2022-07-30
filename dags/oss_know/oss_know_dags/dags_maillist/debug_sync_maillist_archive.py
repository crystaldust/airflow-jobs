import json

from oss_know.libs.maillist.archive import sync_archive

# v0.0.1 It is a mailing list DAG


mail_lists = json.loads('''
[
  {
    "project_name": "apache_mesos",
    "archive_type": "mbox",
    "list_name": "mesos-dev",
    "url_prefix": "https://lists.apache.org/list.html?dev@mesos.apache.org",
    "mbox_path": "./mboxes/apache/mesos"
  },
  {
    "project_name": "azkaban_azkaban",
    "archive_type": "mbox",
    "list_name": "azkaban-dev",
    "url_prefix": "https://lists.apache.org/list.html?dev@azkaban.apache.org",
    "mbox_path": "./mboxes/apache/azkaban"
  },
  {
    "project_name": "apache_ambari",
    "archive_type": "mbox",
    "list_name": "ambari-dev",
    "url_prefix": "https://lists.apache.org/list.html?dev@ambari.apache.org",
    "mbox_path": "./mboxes/apache/ambari"
  },
  {
    "project_name": "apache_zookeeper",
    "archive_type": "mbox",
    "list_name": "zookeeper-dev",
    "url_prefix": "https://lists.apache.org/list.html?dev@zookeeper.apache.org",
    "mbox_path": "./mboxes/apache/zookeeper"
  },
  {
    "project_name": "apache_thrift",
    "archive_type": "mbox",
    "list_name": "thrift-dev",
    "url_prefix": "https://lists.apache.org/list.html?dev@thrift.apache.org",
    "mbox_path": "./mboxes/apache/thrift"
  },
  {
    "project_name": "apache_chukwa",
    "archive_type": "mbox",
    "list_name": "chukwa-dev",
    "url_prefix": "https://lists.apache.org/list.html?dev@chukwa.apache.org",
    "mbox_path": "./mboxes/apache/chukwa"
  },
  {
    "project_name": "lustre_lustre",
    "archive_type": "mbox",
    "list_name": "lustre-dev",
    "url_prefix": "https://lists.apache.org/list.html?dev@lustre.apache.org",
    "mbox_path": "./mboxes/apache/lustre"
  },
  {
    "project_name": "apache_hadoop",
    "archive_type": "mbox",
    "list_name": "hadoop-dev",
    "url_prefix": "https://lists.apache.org/list.html?dev@hadoop.apache.org",
    "mbox_path": "./mboxes/apache/hadoop"
  },
  {
    "project_name": "gluster_glusterfs",
    "archive_type": "mbox",
    "list_name": "glusterfs-dev",
    "url_prefix": "https://lists.apache.org/list.html?dev@glusterfs.apache.org",
    "mbox_path": "./mboxes/apache/glusterfs"
  },
  {
    "project_name": "Alluxio_alluxio",
    "archive_type": "mbox",
    "list_name": "alluxio-dev",
    "url_prefix": "https://lists.apache.org/list.html?dev@alluxio.apache.org",
    "mbox_path": "./mboxes/apache/alluxio"
  },
  {
    "project_name": "ceph_ceph",
    "archive_type": "mbox",
    "list_name": "ceph-dev",
    "url_prefix": "https://lists.apache.org/list.html?dev@ceph.apache.org",
    "mbox_path": "./mboxes/apache/ceph"
  },
  {
    "project_name": "elastic_logstash",
    "archive_type": "mbox",
    "list_name": "logstash-dev",
    "url_prefix": "https://lists.apache.org/list.html?dev@logstash.apache.org",
    "mbox_path": "./mboxes/apache/logstash"
  },
  {
    "project_name": "elastic_elasticsearch",
    "archive_type": "mbox",
    "list_name": "elasticsearch-dev",
    "url_prefix": "https://lists.apache.org/list.html?dev@elasticsearch.apache.org",
    "mbox_path": "./mboxes/apache/elasticsearch"
  },
  {
    "project_name": "elastic_kibana",
    "archive_type": "mbox",
    "list_name": "kibana-dev",
    "url_prefix": "https://lists.apache.org/list.html?dev@kibana.apache.org",
    "mbox_path": "./mboxes/apache/kibana"
  },
  {
    "project_name": "knuckleswtf_scribe",
    "archive_type": "mbox",
    "list_name": "scribe-dev",
    "url_prefix": "https://lists.apache.org/list.html?dev@scribe.apache.org",
    "mbox_path": "./mboxes/apache/scribe"
  },
  {
    "project_name": "apache_flume",
    "archive_type": "mbox",
    "list_name": "flume-dev",
    "url_prefix": "https://lists.apache.org/list.html?dev@flume.apache.org",
    "mbox_path": "./mboxes/apache/flume"
  },
  {
    "project_name": "rabbitmq_rabbitmq-server",
    "archive_type": "mbox",
    "list_name": "rabbitmq-server-dev",
    "url_prefix": "https://lists.apache.org/list.html?dev@rabbitmq-server.apache.org",
    "mbox_path": "./mboxes/apache/rabbitmq-server"
  },
  {
    "project_name": "apache_activemq",
    "archive_type": "mbox",
    "list_name": "activemq-dev",
    "url_prefix": "https://lists.apache.org/list.html?dev@activemq.apache.org",
    "mbox_path": "./mboxes/apache/activemq"
  },
  {
    "project_name": "apache_kafka",
    "archive_type": "mbox",
    "list_name": "kafka-dev",
    "url_prefix": "https://lists.apache.org/list.html?dev@kafka.apache.org",
    "mbox_path": "./mboxes/apache/kafka"
  },
  {
    "project_name": "DHI-GRAS_terracotta",
    "archive_type": "mbox",
    "list_name": "terracotta-dev",
    "url_prefix": "https://lists.apache.org/list.html?dev@terracotta.apache.org",
    "mbox_path": "./mboxes/apache/terracotta"
  },
  {
    "project_name": "apache_ignite",
    "archive_type": "mbox",
    "list_name": "ignite-dev",
    "url_prefix": "https://lists.apache.org/list.html?dev@ignite.apache.org",
    "mbox_path": "./mboxes/apache/ignite"
  },
  {
    "project_name": "apache_spark",
    "archive_type": "mbox",
    "list_name": "spark-dev",
    "url_prefix": "https://lists.apache.org/list.html?dev@spark.apache.org",
    "mbox_path": "./mboxes/apache/spark"
  },
  {
    "project_name": "apache_flink",
    "archive_type": "mbox",
    "list_name": "flink-dev",
    "url_prefix": "https://lists.apache.org/list.html?dev@flink.apache.org",
    "mbox_path": "./mboxes/apache/flink"
  },
  {
    "project_name": "apache_storm",
    "archive_type": "mbox",
    "list_name": "storm-dev",
    "url_prefix": "https://lists.apache.org/list.html?dev@storm.apache.org",
    "mbox_path": "./mboxes/apache/storm"
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

for mail_list_info in mail_lists:
    sync_archive(opensearch_conn_info, **mail_list_info)
