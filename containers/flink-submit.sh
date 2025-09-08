#!/bin/bash

# Flink WebService Pipeline Runner Script (minimal config)

set -e

echo "Starting Flink WebService Pipeline..."

echo "Writing Flink config (REST + slots + defaults)..."
mkdir -p /opt/flink/conf

# Configure TaskManager slots to match job parallelism unless explicitly overridden
: "${FLINK_PARALLELISM:=1}"
: "${FLINK_TASK_SLOTS:=${FLINK_PARALLELISM}}"

cat > /opt/flink/conf/flink-conf.yaml << EOF
EOF

echo "Starting Flink cluster..."
/opt/flink/bin/start-cluster.sh

# Wait for cluster to be ready
sleep 10

# Run Flink job with configured parallelism/slots
 /opt/flink/bin/flink run \
    --detached \
    --class com.marloncalvo.bench.pipelines.FlinkWebServicePipeline \
    /opt/flink-app/pipeline-perf-1.0-SNAPSHOT.jar

# Keep container running and show logs
exec tail -f /opt/flink/log/*.log
