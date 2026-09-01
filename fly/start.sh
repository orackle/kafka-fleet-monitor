#!/usr/bin/env bash
set -e

supervisord -c /etc/supervisord.conf &
SUPERVISOR_PID=$!

echo "waiting for kafka..."
until kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
  sleep 2
done
echo "kafka ready"

kafka-topics --create --if-not-exists --topic system-metrics --bootstrap-server localhost:9092 --partitions 6 --replication-factor 1
kafka-topics --create --if-not-exists --topic anomalies --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
kafka-topics --create --if-not-exists --topic chaos-commands --bootstrap-server localhost:9092 --partitions 6 --replication-factor 1
echo "topics ready"

for svc in api anomaly-detector web-01 web-02 db-01 cache-01 worker-01; do
  supervisorctl -c /etc/supervisord.conf start "$svc"
done

wait $SUPERVISOR_PID
