# Kafka-Cassandra Stream Processor

A Java application that consumes messages from a Kafka topic, transforms them, persists to Cassandra, and produces to another Kafka topic with Exactly-Once Semantics (EOS).

## Features

- **Kafka Streams**: Leverages the Kafka Streams library for processing
- **Exactly-Once Semantics**: Guarantees that each message is processed exactly once
- **Cassandra Persistence**: Stores data in Cassandra with QUORUM consistency level
- **Health Checks**: HTTP endpoint for monitoring application health
- **Prometheus Metrics**: JMX metrics exposed for Prometheus scraping
- **Docker Ready**: Packaged as a Docker image with proper configurations
- **Easy Deployment**: Docker Compose setup for quick deployment

## Requirements

- Docker and Docker Compose
- Java 11 (for development only)
- Maven (for development only)

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/kafka-cassandra-processor.git
cd kafka-cassandra-processor
```

### 2. Run with Docker Compose

```bash
docker-compose up -d
```

This will:
- Start Zookeeper, Kafka, and Cassandra
- Create required Kafka topics
- Set up Cassandra keyspace and table
- Build and start the stream processor application
- Start Prometheus for metrics collection (optional)

### 3. Check Application Status

```bash
curl http://localhost:8080/health
```

You should see a response like:
```json
{"status":"UP","kafka":"UP","cassandra":"UP"}
```

### 4. View Metrics (Optional)

Access Prometheus at http://localhost:9090

## Configuration

The application is configured via properties files mounted into the Docker container.

### Main Configuration File

Edit `config/application.properties` to modify:

- Kafka bootstrap servers and topic names
- Cassandra connection details
- Performance settings

## Development

### Prerequisites

- Java 11 JDK
- Maven 3.6+

### Building the Application

```bash
mvn clean package
```

### Running Tests

#### Unit Tests

```bash
mvn test
```

#### Integration Tests

```bash
docker-compose -f docker-compose.test.yml up --build
```

### Manual Testing

To produce a test message to the source topic:

```bash
# Connect to the Kafka container
docker exec -it kafka bash

# Produce a message
echo '{"id":"test-1","payloadData":"Hello, world!","timestamp":1623456789000}' | \
  kafka-console-producer --broker-list localhost:9092 --topic source_topic
```

To consume from the sink topic:

```bash
# Connect to the Kafka container
docker exec -it kafka bash

# Consume messages
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic sink_topic --from-beginning
```

To check data in Cassandra:

```bash
# Connect to the Cassandra container
docker exec -it cassandra cqlsh

# Query the data
SELECT * FROM app_keyspace.data_table LIMIT 10;
```

## Integration into Existing Systems

To integrate this application into an existing environment:

1. Ensure your environment has Kafka (v3.0+) and Cassandra (v6.8+) available
2. Set up network connectivity between the application and these services
3. Configure `application.properties` to point to your existing services
4. Deploy the application container, ensuring the config volume is mounted

## Production Considerations

For production deployment:

- Use a container orchestration system like Kubernetes
- Set appropriate resource limits for containers
- Configure proper security settings for Kafka and Cassandra
- Review and adjust performance settings
- Set up proper monitoring and alerting
- Configure backups for Cassandra data

## License

This project is licensed under the MIT License - see the LICENSE file for details. 