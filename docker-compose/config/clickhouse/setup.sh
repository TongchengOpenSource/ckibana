#!/bin/bash

# Execute entrypoint as usual after obtaining KEEPER_SERVER_ID
# check KEEPER_SERVER_ID in persistent volume via myid
# if not present, set based on POD hostname
if [[ -f "/bitnami/clickhouse/keeper/data/myid" ]]; then
    export KEEPER_SERVER_ID="$(cat /bitnami/clickhouse/keeper/data/myid)"
else
    HOSTNAME="$(hostname -s)"
    if [[ $HOSTNAME =~ (.*)-([0-9]+)$ ]]; then
        export KEEPER_SERVER_ID=${BASH_REMATCH[2]}
    else
        echo "Failed to get index from hostname $HOST"
        exit 1
    fi
fi
exec /opt/bitnami/scripts/clickhouse/entrypoint.sh /opt/bitnami/scripts/clickhouse/run.sh -- --listen_host=0.0.0.0