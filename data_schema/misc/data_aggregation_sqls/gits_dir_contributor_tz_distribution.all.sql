
create table if not exists gits_dir_contributor_tz_distribution on cluster replicated
(
    ck_data_insert_at Int64,
    search_key__owner String,
    search_key__repo  String,
    in_dir            String,
    author_email      String,
    alter_files_count Int64,
    tz_distribution Array(Map(Int64, UInt64))
)
    engine = Distributed('replicated', 'default', 'gits_dir_contributor_tz_distribution_local', ck_data_insert_at);

create table if not exists gits_dir_contributor_tz_distribution_local on cluster replicated
(
    ck_data_insert_at Int64,
    search_key__owner String,
    search_key__repo  String,
    in_dir            String,
    author_email      String,
    alter_files_count Int64,
    tz_distribution Array(Map(Int64, UInt64))
)
    engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/gits_dir_contributor_tz_distribution', '{replica}')
        PARTITION BY search_key__owner
        ORDER BY (search_key__owner, search_key__repo)
        SETTINGS index_granularity = 8192;