FROM maven:3.8.6-openjdk-11 AS build

# Set working directory
WORKDIR /app

# Copy Maven files
COPY pom.xml .
COPY src ./src/

# Build the application
RUN mvn clean package -DskipTests

# Download the JMX Prometheus Java Agent
RUN mkdir -p /opt/jmx_exporter && \
    curl -L -o /opt/jmx_exporter/jmx_prometheus_javaagent.jar https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.17.0/jmx_prometheus_javaagent-0.17.0.jar

# Second stage: runtime image
FROM openjdk:11-jre-buster

# Set working directory
WORKDIR /app

# Copy application JAR from build stage
COPY --from=build /app/target/kafka-cassandra-processor-*-jar-with-dependencies.jar /app/app.jar
COPY --from=build /opt/jmx_exporter/jmx_prometheus_javaagent.jar /opt/jmx_exporter/jmx_prometheus_javaagent.jar

# Copy configuration files and scripts
COPY config/ /config/
COPY scripts/ /app/scripts/
COPY jmx_exporter_config.yaml /opt/jmx_exporter/jmx_exporter_config.yaml

# Install required tools for health check and scripts
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    python3 \
    && rm -rf /var/lib/apt/lists/*

# Make scripts executable
RUN chmod +x /app/scripts/*.sh

# Expose ports
EXPOSE 8080 9999

# Set up health check
HEALTHCHECK --interval=5s --timeout=3s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Define environment variables
ENV CONFIG_PATH=/config/application.properties
ENV JAVA_OPTS="-Xms512m -Xmx1g"
ENV JMX_EXPORTER="-javaagent:/opt/jmx_exporter/jmx_prometheus_javaagent.jar=9999:/opt/jmx_exporter/jmx_exporter_config.yaml"

# Start the application
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS $JMX_EXPORTER -jar /app/app.jar $CONFIG_PATH"] 