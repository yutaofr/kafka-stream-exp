package com.example.processor;

import com.example.processor.config.AppConfig;
import com.example.processor.health.HealthCheckServer;
import com.example.processor.kafka.TopologyBuilder;
import com.example.processor.persistence.CassandraRepository;
import com.example.processor.transform.MessageTransformer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Main application class that coordinates all components
 * and manages application lifecycle.
 */
public class StreamProcessorApp {
    private static final Logger log = LoggerFactory.getLogger(StreamProcessorApp.class);
    private static final String DEFAULT_CONFIG_PATH = "/config/application.properties";

    /**
     * Main entry point for the application.
     *
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        // Load configuration
        String configPath = args.length > 0 ? args[0] : DEFAULT_CONFIG_PATH;
        log.info("Starting Kafka-Cassandra Stream Processor with config path: {}", configPath);

        try {
            AppConfig appConfig = new AppConfig(configPath);
            run(appConfig);
        } catch (Exception e) {
            log.error("Application failed", e);
            System.exit(1);
        }
    }

    /**
     * Run the application with the specified configuration.
     *
     * @param config Application configuration
     * @throws Exception If the application fails to start
     */
    private static void run(AppConfig config) throws Exception {
        // Create components
        MessageTransformer transformer = new MessageTransformer();
        CassandraRepository cassandraRepository = new CassandraRepository(config);
        TopologyBuilder topologyBuilder = new TopologyBuilder(config, transformer, cassandraRepository);
        
        // Create and configure Kafka Streams
        Properties streamsConfig = createStreamsConfig(config);
        KafkaStreams streams = new KafkaStreams(topologyBuilder.build(), streamsConfig);
        
        // Start health check server
        HealthCheckServer healthCheckServer = new HealthCheckServer(
            config.getHealthCheckPort(), cassandraRepository);
        healthCheckServer.setKafkaStreams(streams);
        healthCheckServer.start();
        
        // Setup exception handler
        streams.setUncaughtExceptionHandler((exception) -> {
            log.error("Uncaught exception in Kafka Streams: ", exception);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        
        // Setup graceful shutdown
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                log.info("Shutdown hook triggered, closing application...");
                streams.close(Duration.ofSeconds(10));
                try {
                    cassandraRepository.close();
                } catch (Exception e) {
                    log.error("Error closing Cassandra repository", e);
                }
                try {
                    healthCheckServer.stop();
                } catch (Exception e) {
                    log.error("Error stopping health check server", e);
                }
                latch.countDown();
                log.info("Application shutdown complete");
            }
        });
        
        // Start Kafka Streams
        streams.start();
        log.info("Kafka Streams started");
        
        // Wait for termination signal
        log.info("Application started successfully!");
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Application interrupted", e);
        }
    }

    /**
     * Creates configuration for Kafka Streams.
     *
     * @param config Application configuration
     * @return Properties for Kafka Streams
     */
    private static Properties createStreamsConfig(AppConfig config) {
        Properties props = new Properties();
        
        // Application identity
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getApplicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
        
        // Performance and stability settings
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, config.getNumStreamThreads());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, config.getCommitIntervalMs());
        props.put(StreamsConfig.STATE_DIR_CONFIG, config.getStateDir());
        
        // Exactly-Once Semantics
        if (config.isExactlyOnce()) {
            props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
            log.info("Exactly-Once Semantics (EOS) enabled");
        } else {
            props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
            log.info("At-least-once semantics enabled (EOS disabled)");
        }
        
        return props;
    }
} 