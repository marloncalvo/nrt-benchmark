# Use official Spark 4.0.1 base image with Java 21 support
FROM apache/spark:4.0.1-scala2.13-java21-ubuntu

USER root

# Set working directory
WORKDIR /opt/spark-app

# Copy Spark-compatible JAR compiled for Java 21
COPY target/pipeline-perf-1.0-SNAPSHOT-spark.jar /opt/spark-app/pipeline-perf-1.0-SNAPSHOT.jar
COPY target/dependencies-spark /opt/spark-app/libs

# Copy and set permissions for the submit script
COPY containers/spark-submit.sh /opt/spark-app/spark-submit.sh
RUN chmod +x /opt/spark-app/spark-submit.sh

# Set Spark and Java options for optimal performance
ENV SPARK_DRIVER_MEMORY=4g \
    SPARK_EXECUTOR_MEMORY=4g \
    SPARK_EXECUTOR_CORES=4 \
    SPARK_DRIVER_CORES=2 \
    SPARK_MAX_CORES=16 \
    SPARK_MASTER="local[*]" \
    SPARK_WORKER_CORES=8 \
    SPARK_EXECUTOR_INSTANCES=2 \
    SPARK_PARALLELISM=32 \
    JAVA_OPTS="-XX:+UseZGC -XX:+ZGenerational -XX:MaxGCPauseMillis=100" \
    JAVA_TOOL_OPTIONS="-XX:+UseZGC -XX:+ZGenerational" \
    CONSUMER_GROUP_ID=spark-pipeline-group

# Set the entry point to run the Spark submit script
ENTRYPOINT ["/opt/spark-app/spark-submit.sh"]