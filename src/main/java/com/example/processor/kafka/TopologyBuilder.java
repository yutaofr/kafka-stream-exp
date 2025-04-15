package com.example.processor.kafka;

import com.example.processor.config.AppConfig;
import com.example.processor.dto.DataTransferObject;
import com.example.processor.persistence.CassandraRepository;
import com.example.processor.transform.MessageTransformer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Optional;

/**
 * Builder for creating the Kafka Streams topology.
 * Defines the stream processing workflow.
 */
public class TopologyBuilder {
    private static final Logger log = LoggerFactory.getLogger(TopologyBuilder.class);
    
    private final AppConfig config;
    private final MessageTransformer transformer;
    private final CassandraRepository cassandraRepository;
    private final ObjectMapper objectMapper;

    /**
     * Creates a new TopologyBuilder.
     * 
     * @param config The application configuration
     * @param transformer The message transformer
     * @param cassandraRepository The Cassandra repository
     */
    public TopologyBuilder(AppConfig config, MessageTransformer transformer, 
                          CassandraRepository cassandraRepository) {
        this.config = config;
        this.transformer = transformer;
        this.cassandraRepository = cassandraRepository;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Builds the Kafka Streams topology.
     * 
     * @return The built topology
     */
    public Topology build() {
        StreamsBuilder builder = new StreamsBuilder();
        
        // Define serialization settings
        Serde<String> stringSerde = Serdes.String();
        
        // Source stream
        KStream<String, String> sourceStream = builder.stream(
            config.getSourceTopic(),
            Consumed.with(stringSerde, stringSerde)
        );
        
        // Transform, persist to Cassandra, and publish to sink topic
        KStream<String, String> processedStream = sourceStream
            .mapValues(this::processMessage)
            .filter((key, value) -> value != null);
        
        // Publish to sink topic
        processedStream.to(
            config.getSinkTopic(),
            Produced.with(stringSerde, stringSerde)
        );
        
        log.info("Kafka Streams topology built successfully");
        return builder.build();
    }

    /**
     * Processes a message by transforming it, persisting to Cassandra, 
     * and preparing for output.
     * 
     * @param value The input message value
     * @return The processed value, or null if processing failed
     */
    private String processMessage(String value) {
        // Transform message
        Optional<DataTransferObject> dtoOpt = transformer.transform(value);
        if (dtoOpt.isEmpty()) {
            log.warn("Message transformation failed, skipping message");
            return null;
        }
        
        DataTransferObject dto = dtoOpt.get();
        
        // Persist to Cassandra
        try {
            cassandraRepository.save(dto);
        } catch (CassandraRepository.CassandraWriteException e) {
            log.error("Failed to persist message to Cassandra, skipping message: {}", 
                     dto.getId(), e);
            return null;
        }
        
        // Serialize for output
        try {
            String outputJson = objectMapper.writeValueAsString(dto);
            log.debug("Message processed successfully: {}", dto.getId());
            return outputJson;
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize DTO for output, skipping message: {}", 
                     dto.getId(), e);
            return null;
        }
    }
} 