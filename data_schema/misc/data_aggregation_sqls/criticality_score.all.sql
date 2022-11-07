create table if not exists criticality_score on cluster replicated
(
    updated_at                Int64,
    owner                     String,
    repo                      String,
    time_point                DateTime64(3),
    name                      String,
    url                       String,
    language                  String,
    description               String,
    created_since             Int64,
    updated_since             Int64,
    contributor_count         Int64,
    watchers_count            Int64,
    org_count                 Int64,
    commit_frequency          Float64,
    recent_releases_count     Int64,
    updated_issues_count      Int64,
    closed_issues_count       Int64,
    comment_frequency         Float64,
    dependents_count          Int64,
    criticality_score         Float64,
    contributor_lookback_days Int32
)
    engine = Distributed('replicated', 'default', 'criticality_score_local', updated_at);

create table if not exists criticality_score_local on cluster replicated
(
    updated_at                Int64,
    owner                     String,
    repo                      String,
    name                      String,
    time_point                DateTime64(3),
    url                       String,
    language                  String,
    description               String,
    created_since             Int64,
    updated_since             Int64,
    contributor_count         Int64,
    watchers_count            Int64,
    org_count                 Int64,
    commit_frequency          Float64,
    recent_releases_count     Int64,
    updated_issues_count      Int64,
    closed_issues_count       Int64,
    comment_frequency         Float64,
    dependents_count          Int64,
    criticality_score         Float64,
    contributor_lookback_days Int32
)
    engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/criticality_score', '{replica}')
        ORDER BY updated_at
        SETTINGS index_granularity = 8192;

