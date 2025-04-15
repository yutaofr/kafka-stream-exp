package com.example.processor.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration loader for the application.
 * Provides access to all configuration properties needed for the application.
 */
public class AppConfig {
    private static final Logger log = LoggerFactory.getLogger(AppConfig.class);
    private final Properties properties;

    /**
     * Loads configuration from a file path.
     * 
     * @param configPath The path to the configuration file
     * @throws IOException If the file cannot be read
     */
    public AppConfig(String configPath) throws IOException {
        log.info("Loading configuration from path: {}", configPath);
        this.properties = new Properties();
        try (InputStream input = new FileInputStream(configPath)) {
            properties.load(input);
        }
        log.info("Configuration loaded successfully");
    }

    // Kafka Settings
    public String getKafkaBootstrapServers() {
        return properties.getProperty("kafka.bootstrap.servers");
    }

    public String getSourceTopic() {
        return properties.getProperty("kafka.topic.source");
    }

    public String getSinkTopic() {
        return properties.getProperty("kafka.topic.sink");
    }

    public String getApplicationId() {
        return properties.getProperty("kafka.streams.application.id", "kafka-cassandra-processor");
    }

    public boolean isExactlyOnce() {
        return Boolean.parseBoolean(properties.getProperty("kafka.streams.exactly.once", "true"));
    }

    public int getCommitIntervalMs() {
        return Integer.parseInt(properties.getProperty("kafka.streams.commit.interval.ms", "1000"));
    }

    public int getNumStreamThreads() {
        return Integer.parseInt(properties.getProperty("kafka.streams.num.stream.threads", "1"));
    }

    public String getStateDir() {
        return properties.getProperty("kafka.streams.state.dir", "/tmp/kafka-streams");
    }

    // Cassandra Settings
    public String getCassandraContactPoints() {
        return properties.getProperty("cassandra.contact-points");
    }

    public int getCassandraPort() {
        return Integer.parseInt(properties.getProperty("cassandra.port", "9042"));
    }

    public String getCassandraKeyspace() {
        return properties.getProperty("cassandra.keyspace");
    }

    public String getCassandraTable() {
        return properties.getProperty("cassandra.table");
    }

    public String getCassandraLocalDc() {
        return properties.getProperty("cassandra.local-datacenter");
    }

    public String getCassandraUsername() {
        return properties.getProperty("cassandra.username", "");
    }

    public String getCassandraPassword() {
        return properties.getProperty("cassandra.password", "");
    }

    // Health Check Settings
    public int getHealthCheckPort() {
        return Integer.parseInt(properties.getProperty("health.check.port", "8080"));
    }

    // JMX Metrics Settings
    public int getPrometheusJmxPort() {
        return Integer.parseInt(properties.getProperty("prometheus.jmx.port", "9999"));
    }
} 