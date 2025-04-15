package com.example.processor.transform;

import com.example.processor.dto.DataTransferObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the MessageTransformer class.
 */
public class MessageTransformerTest {
    
    private MessageTransformer transformer;
    
    @BeforeEach
    void setUp() {
        transformer = new MessageTransformer();
    }
    
    @Test
    void testValidJsonTransformation() {
        // Arrange
        String validJson = "{\"id\":\"test-123\",\"payloadData\":\"Sample data\",\"timestamp\":1623456789000}";
        
        // Act
        Optional<DataTransferObject> result = transformer.transform(validJson);
        
        // Assert
        assertTrue(result.isPresent());
        DataTransferObject dto = result.get();
        assertEquals("test-123", dto.getId());
        assertEquals("Sample data", dto.getPayloadData());
        assertEquals(1623456789000L, dto.getTimestamp());
    }
    
    @Test
    void testInvalidJsonFormat() {
        // Arrange
        String invalidJson = "{invalid-json-format}";
        
        // Act
        Optional<DataTransferObject> result = transformer.transform(invalidJson);
        
        // Assert
        assertTrue(result.isEmpty());
    }
    
    @Test
    void testMissingRequiredFields() {
        // Arrange
        String missingIdJson = "{\"payloadData\":\"Sample data\",\"timestamp\":1623456789000}";
        String missingPayloadDataJson = "{\"id\":\"test-123\",\"timestamp\":1623456789000}";
        
        // Act
        Optional<DataTransferObject> resultMissingId = transformer.transform(missingIdJson);
        Optional<DataTransferObject> resultMissingPayloadData = transformer.transform(missingPayloadDataJson);
        
        // Assert
        assertTrue(resultMissingId.isEmpty());
        assertTrue(resultMissingPayloadData.isEmpty());
    }
    
    @Test
    void testEmptyJsonInput() {
        // Arrange
        String emptyJson = "";
        
        // Act
        Optional<DataTransferObject> result = transformer.transform(emptyJson);
        
        // Assert
        assertTrue(result.isEmpty());
    }
    
    @Test
    void testNullJsonInput() {
        // Act
        Optional<DataTransferObject> result = transformer.transform(null);
        
        // Assert
        assertTrue(result.isEmpty());
    }
} 