#!/bin/bash

# Spark WebService Pipeline Runner Script
# High-performance configuration for Apache Spark 4.0.1

set -e

echo "Starting Spark WebService Pipeline..."

# Set default values from environment
KAFKA_ENDPOINT=${KAFKA_ENDPOINT:-"localhost:9092"}
INPUT_TOPIC=${INPUT_TOPIC:-"input-topic"}
OUTPUT_TOPIC=${OUTPUT_TOPIC:-"output-topic"}
ENDPOINT_BASE_PATH=${ENDPOINT_BASE_PATH:-"http://localhost:5050"}
SPARK_MASTER=${SPARK_MASTER:-"local[*]"}

# Memory configuration
DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-"4g"}
EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY:-"4g"}
EXECUTOR_CORES=${SPARK_EXECUTOR_CORES:-"4"}
MAX_CORES=${SPARK_MAX_CORES:-"16"}

# Create spark events directory if it doesn't exist
mkdir -p /tmp/spark-events

echo "Configuration:"
echo "  KAFKA_ENDPOINT: $KAFKA_ENDPOINT"
echo "  INPUT_TOPIC: $INPUT_TOPIC"
echo "  OUTPUT_TOPIC: $OUTPUT_TOPIC"
echo "  ENDPOINT_BASE_PATH: $ENDPOINT_BASE_PATH"
echo "  SPARK_MASTER: $SPARK_MASTER"
echo "  DRIVER_MEMORY: $DRIVER_MEMORY"
echo "  EXECUTOR_MEMORY: $EXECUTOR_MEMORY"

# Run Spark submit with optimized configurations
exec /opt/spark/bin/spark-submit \
    --master "$SPARK_MASTER" \
    --driver-memory "$DRIVER_MEMORY" \
    --executor-memory "$EXECUTOR_MEMORY" \
    --executor-cores "$EXECUTOR_CORES" \
    --conf spark.cores.max="$MAX_CORES" \
    --conf spark.driver.extraJavaOptions="-XX:+UseZGC -XX:+ZGenerational" \
    --conf spark.driver.extraClassPath="/opt/spark-app/libs/*" \
    --conf spark.executor.extraClassPath="/opt/spark-app/libs/*" \
    --conf spark.executor.extraJavaOptions="-XX:+UseZGC -XX:+ZGenerational" \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.sql.adaptive.skewJoin.enabled=true \
    --conf spark.sql.adaptive.localShuffleReader.enabled=true \
    --conf spark.sql.streaming.metricsEnabled=true \
    --conf spark.sql.streaming.numPartitions=32 \
    --conf spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider \
    --conf spark.sql.streaming.kafka.consumer.cache.enabled=true \
    --conf spark.sql.streaming.kafka.consumer.cache.capacity=10000 \
    --conf spark.sql.streaming.kafka.consumer.cache.evictorThreadRunIntervalMs=60000 \
    --conf spark.sql.shuffle.partitions=32 \
    --conf spark.shuffle.compress=true \
    --conf spark.shuffle.spill.compress=true \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryo.unsafe=true \
    --conf spark.kryo.registrationRequired=false \
    --conf spark.rpc.message.maxSize=1024 \
    --conf spark.network.timeout=300s \
    --conf spark.memory.fraction=0.75 \
    --conf spark.memory.storageFraction=0.30 \
    --conf spark.ui.enabled=true \
    --conf spark.ui.port=4040 \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=/tmp/spark-events \
    --class com.marloncalvo.bench.pipelines.SparkWebServicePipeline \
    /opt/spark-app/pipeline-perf-1.0-SNAPSHOT.jar