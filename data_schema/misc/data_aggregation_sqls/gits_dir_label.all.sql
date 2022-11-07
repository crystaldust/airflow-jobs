create table if not exists gits_dir_label_local on cluster replicated
(
    ck_data_insert_at Int64,
    search_key__owner String,
    search_key__repo  String,
    author_tz         Int64,
    committer_tz      Int64,
    author_name       String,
    author_email      String,
    authored_date     DateTime64(3),
    committer_name    String,
    committer_email   String,
    committed_date    DateTime64(3),
    dir_list Array(String),
    array_slice Array(String),
    in_dir            String
)
    engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/gits_dir_label', '{replica}')
        PARTITION BY search_key__owner
        ORDER BY (search_key__owner, search_key__repo)
        SETTINGS index_granularity = 8192;


create table if not exists gits_dir_label on cluster replicated
(
    ck_data_insert_at Int64,
    search_key__owner String,
    search_key__repo  String,
    author_tz         Int64,
    committer_tz      Int64,
    author_name       String,
    author_email      String,
    authored_date     DateTime64(3),
    committer_name    String,
    committer_email   String,
    committed_date    DateTime64(3),
    dir_list Array(String),
    array_slice Array(String),
    in_dir            String
)
    engine = Distributed('replicated', 'default', 'gits_dir_label_local', ck_data_insert_at);
