create table if not exists gits_dir_email_domain_contributer_count on cluster replicated
(
    ck_data_insert_at Int64,
    search_key__owner String,
    search_key__repo  String,
    in_dir            String,
    authored_date     Int64,
    email_domain      String,
    contributer_count Int64
)
    engine = Distributed('replicated', 'default', 'gits_dir_email_domain_contributer_count_local', ck_data_insert_at);


create table if not exists gits_dir_email_domain_contributer_count_local on cluster replicated
(
    ck_data_insert_at Int64,
    search_key__owner String,
    search_key__repo  String,
    in_dir            String,
    authored_date     Int64,
    email_domain      String,
    contributer_count Int64
)
    engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/gits_dir_email_domain_contributer_count_', '{replica}')
        PARTITION BY search_key__owner
        ORDER BY (search_key__owner, search_key__repo)
        SETTINGS index_granularity = 8192;
