URL="http://elasticsearch:9200/_bulk"
DELAY=5
while true; do
    status_code=$( \
    curl -sL -m 5 -w "%{http_code}"  -o /dev/null -XPOST "$URL"  -H 'Content-Type: application/json' -d'
{ "index": { "_index" : "proxy-settings", "_type" : "info", "_id": "kibana"}}
{ "key":"kibana", "value": "  query:\n    sampleIndexPatterns:\n      - ops_nginx_all\n    sampleCountMaxThreshold: 1500000\n    useCache: true\n  proxy:\n    roundAbleMinPeriod: 120000\n    maxTimeRange: 86400000\n    blackIndexList:\n    whiteIndexList:\n      - ops_nginx_all\n    ck:\n      url: clickhouse-1:8123\n      user: default\n      pass: default\n      defaultCkDatabase: ops"}
' )
    echo $status_code
    if [[ "$status_code" == "200" ]]; then
        echo "Success! Got 200 OK response from $URL"
        break
    else
        echo "Still waiting for 200 OK response from $URL (got $status_code)"
    fi
    sleep $DELAY
done
while true; do
    sleep 60
done