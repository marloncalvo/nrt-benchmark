FROM flink:2.1-java21

USER root

# Set working directory
WORKDIR /opt/flink-app

# Copy Flink-compatible JAR compiled for Java 21
COPY target/dependencies-flink/* /opt/flink/lib/
COPY containers/flink-submit.sh /opt/flink-app/flink-submit.sh
RUN chmod +x /opt/flink-app/flink-submit.sh

COPY target/pipeline-perf-1.0-SNAPSHOT-flink.jar /opt/flink-app/pipeline-perf-1.0-SNAPSHOT.jar

# Set the entry point to run the Flink submit script
ENTRYPOINT ["/opt/flink-app/flink-submit.sh"]
