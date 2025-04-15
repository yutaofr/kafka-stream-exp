#!/bin/bash
# Script to create Kafka topics required for the application

# Default values
BOOTSTRAP_SERVERS="kafka:9092"
SOURCE_TOPIC="source_topic"
SINK_TOPIC="sink_topic"
PARTITIONS=3
REPLICATION_FACTOR=3

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --bootstrap-servers)
      BOOTSTRAP_SERVERS="$2"
      shift 2
      ;;
    --source-topic)
      SOURCE_TOPIC="$2"
      shift 2
      ;;
    --sink-topic)
      SINK_TOPIC="$2"
      shift 2
      ;;
    --partitions)
      PARTITIONS="$2"
      shift 2
      ;;
    --replication-factor)
      REPLICATION_FACTOR="$2"
      shift 2
      ;;
    --help)
      echo "Usage: $0 [OPTIONS]"
      echo "Options:"
      echo "  --bootstrap-servers    Kafka bootstrap servers (default: kafka:9092)"
      echo "  --source-topic         Source topic name (default: source_topic)"
      echo "  --sink-topic           Sink topic name (default: sink_topic)"
      echo "  --partitions           Number of partitions (default: 3)"
      echo "  --replication-factor   Replication factor (default: 3)"
      echo "  --help                 Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

echo "Creating Kafka topics with the following configuration:"
echo "Bootstrap Servers: $BOOTSTRAP_SERVERS"
echo "Source Topic: $SOURCE_TOPIC"
echo "Sink Topic: $SINK_TOPIC"
echo "Partitions: $PARTITIONS"
echo "Replication Factor: $REPLICATION_FACTOR"

# Create source topic
echo "Creating source topic: $SOURCE_TOPIC"
kafka-topics --bootstrap-server $BOOTSTRAP_SERVERS \
  --create \
  --if-not-exists \
  --topic $SOURCE_TOPIC \
  --partitions $PARTITIONS \
  --replication-factor $REPLICATION_FACTOR \
  --config cleanup.policy=delete \
  --config retention.ms=604800000

# Create sink topic
echo "Creating sink topic: $SINK_TOPIC"
kafka-topics --bootstrap-server $BOOTSTRAP_SERVERS \
  --create \
  --if-not-exists \
  --topic $SINK_TOPIC \
  --partitions $PARTITIONS \
  --replication-factor $REPLICATION_FACTOR \
  --config cleanup.policy=delete \
  --config retention.ms=604800000

# Verify topics were created
echo "Verifying topics were created:"
kafka-topics --bootstrap-server $BOOTSTRAP_SERVERS --list | grep -E "$SOURCE_TOPIC|$SINK_TOPIC"

echo "Topic creation complete!" 