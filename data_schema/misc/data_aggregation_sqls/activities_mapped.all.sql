create table if not exists activities_mapped on cluster replicated
(
    owner              String,
    repo               String,
    github_id          Int64,
    github_login       String,
    knowledge_sharing  Float64,
    code_contribution  Float64,
    issue_coordination Float64,
    progress_control   Float64,
    code_tweaking      Float64,
    issue_reporting    Float64
)
    engine = Distributed('replicated', 'default', 'activities_mapped_local', github_id);

create table if not exists activities_mapped_local on cluster replicated
(
    owner              String,
    repo               String,
    github_id          Int64,
    github_login       String,
    knowledge_sharing  Float64,
    code_contribution  Float64,
    issue_coordination Float64,
    progress_control   Float64,
    code_tweaking      Float64,
    issue_reporting    Float64
)
    engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/activities_mapped', '{replica}')
        PARTITION BY owner
        ORDER BY (owner, repo, github_id)
        SETTINGS index_granularity = 8192;

