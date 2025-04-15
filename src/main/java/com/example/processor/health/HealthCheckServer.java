package com.example.processor.health;

import com.example.processor.persistence.CassandraRepository;
import org.apache.kafka.streams.KafkaStreams;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * HTTP server that exposes a health check endpoint.
 * This allows external systems (like Docker) to monitor the application health.
 */
public class HealthCheckServer {
    private static final Logger log = LoggerFactory.getLogger(HealthCheckServer.class);
    
    private final Server server;
    private final CassandraRepository cassandraRepository;
    private final AtomicReference<KafkaStreams> kafkaStreamsRef;

    /**
     * Creates a new health check server.
     * 
     * @param port The port to listen on
     * @param cassandraRepository The Cassandra repository to check health
     */
    public HealthCheckServer(int port, CassandraRepository cassandraRepository) {
        this.server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(port);
        server.addConnector(connector);
        
        this.cassandraRepository = cassandraRepository;
        this.kafkaStreamsRef = new AtomicReference<>();
        
        // Setup servlet handler
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        server.setHandler(context);
        
        // Register health check servlet
        context.addServlet(new ServletHolder(new HealthCheckServlet()), "/health");
        
        log.info("Health check server configured on port {}", port);
    }

    /**
     * Sets the Kafka Streams instance to monitor.
     * 
     * @param streams The Kafka Streams instance
     */
    public void setKafkaStreams(KafkaStreams streams) {
        this.kafkaStreamsRef.set(streams);
    }

    /**
     * Starts the health check server.
     * 
     * @throws Exception If the server fails to start
     */
    public void start() throws Exception {
        server.start();
        log.info("Health check server started on port {}", 
                ((ServerConnector)server.getConnectors()[0]).getPort());
    }

    /**
     * Stops the health check server.
     * 
     * @throws Exception If the server fails to stop
     */
    public void stop() throws Exception {
        log.info("Stopping health check server");
        server.stop();
    }

    /**
     * Servlet that handles health check requests.
     */
    private class HealthCheckServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) 
                throws IOException {
            log.debug("Health check request received");
            
            // Check Kafka Streams health
            KafkaStreams streams = kafkaStreamsRef.get();
            boolean kafkaHealthy = streams != null && 
                                   streams.state() == KafkaStreams.State.RUNNING;
            
            // Check Cassandra health
            boolean cassandraHealthy = cassandraRepository.isHealthy();
            
            // Overall health is the combination of both
            boolean isHealthy = kafkaHealthy && cassandraHealthy;
            
            // Prepare response
            resp.setContentType("application/json");
            
            if (isHealthy) {
                resp.setStatus(HttpServletResponse.SC_OK);
                resp.getWriter().write("{\"status\":\"UP\",\"kafka\":\"UP\",\"cassandra\":\"UP\"}");
            } else {
                resp.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
                resp.getWriter().write(String.format(
                    "{\"status\":\"DOWN\",\"kafka\":\"%s\",\"cassandra\":\"%s\"}",
                    kafkaHealthy ? "UP" : "DOWN",
                    cassandraHealthy ? "UP" : "DOWN"
                ));
            }
            
            log.debug("Health check response: kafka={}, cassandra={}, overall={}",
                    kafkaHealthy ? "UP" : "DOWN",
                    cassandraHealthy ? "UP" : "DOWN",
                    isHealthy ? "UP" : "DOWN");
        }
    }
} 