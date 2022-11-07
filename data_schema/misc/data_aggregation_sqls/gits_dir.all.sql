create table if not exists gits_dir on cluster replicated
(
    ck_data_insert_at Int64,
    search_key__owner String,
    search_key__repo  String,
    dir               String
)
    engine = Distributed('replicated', 'default', 'gits_dir_local', ck_data_insert_at);



create table if not exists gits_dir_local on cluster replicated
(
    ck_data_insert_at Int64,
    search_key__owner String,
    search_key__repo  String,
    dir               String
)
    engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/gits_dir', '{replica}')
        ORDER BY (search_key__owner, search_key__repo)
        SETTINGS index_granularity = 8192;
