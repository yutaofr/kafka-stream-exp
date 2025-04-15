package com.example.processor.persistence;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.example.processor.config.AppConfig;
import com.example.processor.dto.DataTransferObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Repository for interacting with Cassandra database.
 * Handles connection management and data persistence operations.
 */
public class CassandraRepository implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(CassandraRepository.class);

    private final CqlSession session;
    private final PreparedStatement insertStatement;
    private final PreparedStatement healthCheckStatement;
    private final String keyspace;
    private final String table;

    /**
     * Creates a new CassandraRepository with the provided configuration.
     * 
     * @param config The application configuration
     */
    public CassandraRepository(AppConfig config) {
        this.keyspace = config.getCassandraKeyspace();
        this.table = config.getCassandraTable();
        String contactPointsStr = config.getCassandraContactPoints();
        int port = config.getCassandraPort();
        String localDc = config.getCassandraLocalDc();
        String username = config.getCassandraUsername();
        String password = config.getCassandraPassword();

        log.info("Initializing Cassandra connection to {} with keyspace {}", contactPointsStr, keyspace);

        // Parse contact points
        List<InetSocketAddress> contactPoints = Stream.of(contactPointsStr.split(","))
                .map(String::trim)
                .map(host -> new InetSocketAddress(host, port))
                .collect(Collectors.toList());

        // Build session with appropriate settings
        this.session = CqlSession.builder()
                .addContactPoints(contactPoints)
                .withLocalDatacenter(localDc)
                .withKeyspace(this.keyspace)
                .build();

        // Prepare statements for reuse
        this.insertStatement = session.prepare(
            String.format("INSERT INTO %s.%s (id, payload_data, timestamp) VALUES (?, ?, ?)",
                         keyspace, table)
        );
        
        this.healthCheckStatement = session.prepare("SELECT now() FROM system.local");
        
        log.info("CassandraRepository initialized. Connected to keyspace '{}', table '{}'", keyspace, table);
    }

    /**
     * Saves the DTO to Cassandra with QUORUM consistency.
     * 
     * @param dto The DataTransferObject to persist
     * @throws CassandraWriteException If the write operation fails
     */
    public void save(DataTransferObject dto) throws CassandraWriteException {
        log.debug("Attempting to save DTO with id: {}", dto.getId());
        
        BoundStatement boundStatement = insertStatement.bind(
                dto.getId(),
                dto.getPayloadData(),
                dto.getTimestamp()
        ).setConsistencyLevel(ConsistencyLevel.QUORUM);

        try {
            ResultSet resultSet = session.execute(boundStatement);
            // We could check resultSet.wasApplied() if using LWT queries
            log.debug("Successfully saved DTO with id: {}", dto.getId());
        } catch (UnavailableException e) {
            String msg = String.format(
                "Cassandra Write Failed (Unavailable): Not enough replicas available for QUORUM. " +
                "Needed %d, Got %d. DTO ID: %s", 
                e.getRequired(), e.getAlive(), dto.getId());
            log.error(msg, e);
            throw new CassandraWriteException(msg, e);
        } catch (WriteTimeoutException e) {
            String msg = String.format(
                "Cassandra Write Failed (Timeout): Write operation timed out. DTO ID: %s",
                dto.getId());
            log.error(msg, e);
            throw new CassandraWriteException(msg, e);
        } catch (Exception e) {
            String msg = String.format("Cassandra Write Failed: %s. DTO ID: %s", 
                                      e.getMessage(), dto.getId());
            log.error(msg, e);
            throw new CassandraWriteException(msg, e);
        }
    }

    /**
     * Checks if the connection to Cassandra is healthy.
     * 
     * @return true if the connection is healthy, false otherwise
     */
    public boolean isHealthy() {
        try {
            session.execute(healthCheckStatement.bind());
            return true;
        } catch (Exception e) {
            log.error("Health check failed", e);
            return false;
        }
    }

    @Override
    public void close() {
        if (session != null && !session.isClosed()) {
            log.info("Closing Cassandra session");
            session.close();
        }
    }

    /**
     * Exception thrown when a Cassandra write operation fails.
     */
    public static class CassandraWriteException extends Exception {
        public CassandraWriteException(String message, Throwable cause) {
            super(message, cause);
        }
    }
} 