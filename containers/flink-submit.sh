#!/bin/bash

# Flink WebService Pipeline Runner Script (minimal config)

set -e

echo "Starting Flink WebService Pipeline..."

# Write to config file
cat <<EOF > /opt/flink/conf/config.yaml
taskmanager:
  memory:
    process:
      size: 1600m
  bind-host: 0.0.0.0
  numberOfTaskSlots: 10
jobmanager:
  bind-host: 0.0.0.0
  execution:
    failover-strategy: region
  rpc:
    address: localhost
    port: 6123
  memory:
    process:
      size: 1600m
parallelism:
  default: 6
rest:
  address: 0.0.0.0
  # flamegraph:
  #   enabled: true
  # profiling:
  #   enabled: true
env:
  java:
    opts:
      all: --add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED
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
