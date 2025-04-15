package com.example.processor.integration;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.example.processor.config.AppConfig;
import com.example.processor.dto.DataTransferObject;
import com.example.processor.kafka.TopologyBuilder;
import com.example.processor.persistence.CassandraRepository;
import com.example.processor.transform.MessageTransformer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for the complete stream processing flow.
 * Uses TestContainers to spin up real Kafka and Cassandra instances.
 */
@Testcontainers
public class StreamProcessorIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(StreamProcessorIntegrationTest.class);
    
    private static final String SOURCE_TOPIC = "test-source-topic";
    private static final String SINK_TOPIC = "test-sink-topic";
    private static final String KEYSPACE = "test_keyspace";
    private static final String TABLE = "test_data_table";
    
    private static File configFile;
    private static KafkaStreams streams;
    private static CassandraRepository cassandraRepository;
    private static CqlSession cqlSession;
    
    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-server:7.5.8"));
    
    @Container
    private static final CassandraContainer<?> CASSANDRA = new CassandraContainer<>(
            DockerImageName.parse("datastax/dse-server:6.8.9"));
    
    @BeforeAll
    static void setup() throws Exception {
        // Create test config file
        createTestConfigFile();
        
        // Create Cassandra keyspace and table
        setupCassandra();
        
        // Create Kafka topics
        setupKafka();
        
        // Start the stream processor
        AppConfig appConfig = new AppConfig(configFile.getAbsolutePath());
        startStreamProcessor(appConfig);
        
        // Wait for everything to initialize
        TimeUnit.SECONDS.sleep(5);
    }
    
    @AfterAll
    static void tearDown() {
        if (streams != null) {
            streams.close();
        }
        
        if (cassandraRepository != null) {
            try {
                cassandraRepository.close();
            } catch (Exception e) {
                log.error("Error closing Cassandra repository", e);
            }
        }
        
        if (cqlSession != null) {
            cqlSession.close();
        }
        
        if (configFile != null && configFile.exists()) {
            configFile.delete();
        }
    }
    
    @Test
    void testEndToEndProcessing() throws Exception {
        // Create test data
        String testId = "test-" + UUID.randomUUID();
        String testPayload = "Test payload data";
        long testTimestamp = System.currentTimeMillis();
        
        DataTransferObject testDto = new DataTransferObject(testId, testPayload, testTimestamp);
        String inputJson = new ObjectMapper().writeValueAsString(testDto);
        
        // Send message to source topic
        sendMessageToKafka(SOURCE_TOPIC, testId, inputJson);
        
        // Wait for processing
        TimeUnit.SECONDS.sleep(5);
        
        // Verify message in sink topic
        String outputJson = consumeMessageFromKafka(SINK_TOPIC);
        assertNotNull(outputJson, "Should receive message in sink topic");
        
        DataTransferObject receivedDto = new ObjectMapper().readValue(outputJson, DataTransferObject.class);
        assertEquals(testId, receivedDto.getId());
        assertEquals(testPayload, receivedDto.getPayloadData());
        assertEquals(testTimestamp, receivedDto.getTimestamp());
        
        // Verify data in Cassandra
        Row row = cqlSession.execute(
            String.format("SELECT * FROM %s.%s WHERE id = '%s'", KEYSPACE, TABLE, testId)
        ).one();
        
        assertNotNull(row, "Data should be persisted in Cassandra");
        assertEquals(testId, row.getString("id"));
        assertEquals(testPayload, row.getString("payload_data"));
        assertEquals(testTimestamp, row.getLong("timestamp"));
    }
    
    /**
     * Sets up the Cassandra keyspace and table.
     */
    private static void setupCassandra() {
        cqlSession = CqlSession.builder()
            .addContactPoint(new InetSocketAddress(CASSANDRA.getHost(), CASSANDRA.getMappedPort(9042)))
            .withLocalDatacenter("datacenter1")
            .build();
        
        // Create keyspace
        cqlSession.execute(
            "CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE +
            " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        
        // Create table
        cqlSession.execute(
            "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + TABLE +
            " (id text PRIMARY KEY, payload_data text, timestamp bigint)");
        
        log.info("Cassandra keyspace and table created");
    }
    
    /**
     * Sets up Kafka topics.
     */
    private static void setupKafka() {
        Properties adminProps = new Properties();
        adminProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        
        // Create topics using Kafka admin client
        org.apache.kafka.clients.admin.AdminClient adminClient = 
            org.apache.kafka.clients.admin.AdminClient.create(adminProps);
        
        adminClient.createTopics(java.util.Arrays.asList(
            new org.apache.kafka.clients.admin.NewTopic(SOURCE_TOPIC, 1, (short) 1),
            new org.apache.kafka.clients.admin.NewTopic(SINK_TOPIC, 1, (short) 1)
        ));
        
        adminClient.close();
        log.info("Kafka topics created");
    }
    
    /**
     * Creates a test configuration file.
     */
    private static void createTestConfigFile() throws IOException {
        configFile = File.createTempFile("test-application", ".properties");
        
        try (FileWriter writer = new FileWriter(configFile)) {
            writer.write("# Kafka Settings\n");
            writer.write("kafka.bootstrap.servers=" + KAFKA.getBootstrapServers() + "\n");
            writer.write("kafka.topic.source=" + SOURCE_TOPIC + "\n");
            writer.write("kafka.topic.sink=" + SINK_TOPIC + "\n");
            writer.write("kafka.streams.application.id=test-kafka-cassandra-processor\n");
            writer.write("kafka.streams.exactly.once=true\n");
            writer.write("kafka.streams.commit.interval.ms=100\n");
            writer.write("kafka.streams.num.stream.threads=1\n");
            writer.write("kafka.streams.state.dir=/tmp/kafka-streams-test\n");
            
            writer.write("\n# Cassandra Settings\n");
            writer.write("cassandra.contact-points=" + CASSANDRA.getHost() + "\n");
            writer.write("cassandra.port=" + CASSANDRA.getMappedPort(9042) + "\n");
            writer.write("cassandra.keyspace=" + KEYSPACE + "\n");
            writer.write("cassandra.table=" + TABLE + "\n");
            writer.write("cassandra.local-datacenter=datacenter1\n");
            
            writer.write("\n# Health Check Settings\n");
            writer.write("health.check.port=8081\n");
        }
        
        log.info("Test config file created at: {}", configFile.getAbsolutePath());
    }
    
    /**
     * Starts the stream processor with the given configuration.
     */
    private static void startStreamProcessor(AppConfig config) {
        // Create components
        MessageTransformer transformer = new MessageTransformer();
        cassandraRepository = new CassandraRepository(config);
        TopologyBuilder topologyBuilder = new TopologyBuilder(config, transformer, cassandraRepository);
        
        // Create Kafka Streams config
        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getApplicationId());
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, 
                         "org.apache.kafka.common.serialization.Serdes$StringSerde");
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, 
                         "org.apache.kafka.common.serialization.Serdes$StringSerde");
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, config.getCommitIntervalMs());
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        
        // Start the streams
        streams = new KafkaStreams(topologyBuilder.build(), streamsConfig);
        streams.start();
        
        log.info("Stream processor started for testing");
    }
    
    /**
     * Sends a message to the specified Kafka topic.
     */
    private void sendMessageToKafka(String topic, String key, String value) 
            throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record).get();
            log.info("Sent test message to topic {}: key={}, value={}", topic, key, value);
        }
    }
    
    /**
     * Consumes a message from the specified Kafka topic.
     */
    private String consumeMessageFromKafka(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            
            // Poll for messages
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Received test message from topic {}: key={}, value={}", 
                            topic, record.key(), record.value());
                    return record.value();
                }
            }
        }
        
        return null;
    }
} 