package com.example.processor.transform;

import com.example.processor.dto.DataTransferObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Transforms JSON messages into DataTransferObject instances.
 * This class implements pure transformation logic with no side effects.
 */
public class MessageTransformer {
    private static final Logger log = LoggerFactory.getLogger(MessageTransformer.class);
    private final ObjectMapper objectMapper;

    /**
     * Creates a new MessageTransformer with a configured ObjectMapper.
     */
    public MessageTransformer() {
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Transforms a JSON string payload into a DataTransferObject.
     * This is a pure function that doesn't produce side effects.
     * 
     * @param jsonPayload The input JSON string.
     * @return Optional containing the DTO if transformation is successful, empty otherwise.
     */
    public Optional<DataTransferObject> transform(String jsonPayload) {
        if (jsonPayload == null || jsonPayload.isEmpty()) {
            log.warn("Received null or empty payload");
            return Optional.empty();
        }
        
        try {
            // Deserialize JSON into DataTransferObject
            DataTransferObject dto = objectMapper.readValue(jsonPayload, DataTransferObject.class);
            
            // Validate essential fields
            if (dto.getId() == null || dto.getId().isEmpty()) {
                log.warn("Validation failed: ID is missing in payload: {}", jsonPayload);
                return Optional.empty();
            }
            
            if (dto.getPayloadData() == null || dto.getPayloadData().isEmpty()) {
                log.warn("Validation failed: PayloadData is missing in payload: {}", jsonPayload);
                return Optional.empty();
            }
            
            return Optional.of(dto);
        } catch (JsonProcessingException e) {
            log.error("Failed to parse JSON payload: {}", jsonPayload, e);
            return Optional.empty();
        } catch (Exception e) {
            log.error("Failed to transform payload: {}", jsonPayload, e);
            return Optional.empty();
        }
    }
} 