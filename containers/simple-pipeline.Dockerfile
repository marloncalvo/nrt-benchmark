FROM eclipse-temurin:24-jre
WORKDIR /app
COPY target/pipeline-perf-1.0-SNAPSHOT.jar .
COPY target/dependency /app/lib
ENTRYPOINT ["java", "-cp", "pipeline-perf-1.0-SNAPSHOT.jar:lib/*", "com.marloncalvo.bench.pipelines.SimplePipeline"]
