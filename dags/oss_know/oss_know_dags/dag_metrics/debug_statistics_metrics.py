import json

from oss_know.libs.metrics.init_statistics_metrics import statistics_metrics, statistics_activities, activities_mapped

clickhouse_server_info = json.loads('''
{
  "CLUSTER_NAME": "replicated",
  "DATABASE": "default",
  "HOST": "127.0.0.1",
  "PASSWD": "default",
  "PORT": "19000",
  "USER": "default"
}
''')

statistics_metrics(clickhouse_server_info=clickhouse_server_info)
statistics_activities(clickhouse_server_info=clickhouse_server_info)
activities_mapped(clickhouse_server_info=clickhouse_server_info)

# TODO Add the calculation of mapped activities(mapping values to range[0, 100])
