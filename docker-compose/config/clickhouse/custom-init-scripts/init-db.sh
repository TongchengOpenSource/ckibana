#!/bin/bash
clickhouse-client --user=default --password=default --query="create database ops"

clickhouse-client --user=default --password=default << 'EOF'
CREATE TABLE ops.ops_nginx_local
(
`request_method` LowCardinality(String),
`request_uri` String,
`status` Int64,
`request_time` Float64,
`request_length` Int64,
`http_referer` String,
`http_user_agent` String,
`http_host` String,
`request_id` String,
`remote_addr` String,
`bytes_sent` Int64,
`body_bytes_sent` Int64,
`@timestamp` UInt64,
`ck_assembly_extension` String,
INDEX timestamp_index `@timestamp` TYPE minmax GRANULARITY 8192
)
ENGINE = MergeTree
PARTITION BY (toYYYYMMDD(toDateTime(`@timestamp` / 1000, 'Asia/Shanghai')), toHour(toDateTime(`@timestamp` / 1000, 'Asia/Shanghai')))
ORDER BY (`http_host`, intHash64(`@timestamp`))
SAMPLE BY intHash64(`@timestamp`)
SETTINGS in_memory_parts_enable_wal = 0, index_granularity = 8192
EOF

clickhouse-client --user=default --password=default << 'EOF'
CREATE TABLE IF NOT EXISTS ops.ops_nginx_all AS ops.ops_nginx_local ENGINE Distributed('default', 'ops', 'ops_nginx_local', rand());
EOF
