package com.example.processor.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * Data transfer object representing the message payload.
 * This class is immutable to ensure data consistency.
 */
public final class DataTransferObject {
    private final String id;
    private final String payloadData;
    private final long timestamp;

    /**
     * Creates a new DataTransferObject.
     * 
     * @param id The unique identifier
     * @param payloadData The main payload content
     * @param timestamp The timestamp of when the data was created
     */
    @JsonCreator
    public DataTransferObject(
            @JsonProperty("id") String id,
            @JsonProperty("payloadData") String payloadData,
            @JsonProperty("timestamp") long timestamp) {
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.payloadData = Objects.requireNonNull(payloadData, "payloadData must not be null");
        this.timestamp = timestamp;
    }

    /**
     * Gets the unique identifier.
     * 
     * @return The ID
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the payload content.
     * 
     * @return The payload data
     */
    public String getPayloadData() {
        return payloadData;
    }

    /**
     * Gets the timestamp.
     * 
     * @return The timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DataTransferObject other = (DataTransferObject) obj;
        return timestamp == other.timestamp &&
               Objects.equals(id, other.id) &&
               Objects.equals(payloadData, other.payloadData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, payloadData, timestamp);
    }

    @Override
    public String toString() {
        return "DataTransferObject{" +
               "id='" + id + '\'' +
               ", payloadData='" + payloadData + '\'' +
               ", timestamp=" + timestamp +
               '}';
    }
} 