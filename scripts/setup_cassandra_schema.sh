#!/bin/bash
# Script to set up Cassandra schema for the application

# Default values
CASSANDRA_HOST="cassandra"
CASSANDRA_PORT=9042
CASSANDRA_USERNAME=""
CASSANDRA_PASSWORD=""
SCHEMA_FILE="schema.cql"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --host)
      CASSANDRA_HOST="$2"
      shift 2
      ;;
    --port)
      CASSANDRA_PORT="$2"
      shift 2
      ;;
    --username)
      CASSANDRA_USERNAME="$2"
      shift 2
      ;;
    --password)
      CASSANDRA_PASSWORD="$2"
      shift 2
      ;;
    --schema-file)
      SCHEMA_FILE="$2"
      shift 2
      ;;
    --help)
      echo "Usage: $0 [OPTIONS]"
      echo "Options:"
      echo "  --host          Cassandra host (default: cassandra)"
      echo "  --port          Cassandra port (default: 9042)"
      echo "  --username      Cassandra username (default: none)"
      echo "  --password      Cassandra password (default: none)"
      echo "  --schema-file   Path to schema CQL file (default: schema.cql)"
      echo "  --help          Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Check if the schema file exists
if [ ! -f "$SCHEMA_FILE" ]; then
  # Check if the schema file exists in the scripts directory
  SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
  if [ -f "$SCRIPT_DIR/$SCHEMA_FILE" ]; then
    SCHEMA_FILE="$SCRIPT_DIR/$SCHEMA_FILE"
  else
    echo "Schema file not found: $SCHEMA_FILE"
    exit 1
  fi
fi

echo "Setting up Cassandra schema with the following configuration:"
echo "Host: $CASSANDRA_HOST"
echo "Port: $CASSANDRA_PORT"
echo "Schema File: $SCHEMA_FILE"

# Build the CQLSH command with authentication if provided
CQLSH_CMD="cqlsh $CASSANDRA_HOST $CASSANDRA_PORT"
if [ -n "$CASSANDRA_USERNAME" ] && [ -n "$CASSANDRA_PASSWORD" ]; then
  CQLSH_CMD="$CQLSH_CMD -u $CASSANDRA_USERNAME -p $CASSANDRA_PASSWORD"
fi

# Wait for Cassandra to be ready
MAX_RETRIES=30
RETRY_INTERVAL=5
RETRIES=0

echo "Waiting for Cassandra to be ready..."
until $CQLSH_CMD -e "DESCRIBE KEYSPACES" > /dev/null 2>&1 || [ $RETRIES -eq $MAX_RETRIES ]; do
  echo "Waiting for Cassandra connection... ($((RETRIES+1))/$MAX_RETRIES)"
  sleep $RETRY_INTERVAL
  RETRIES=$((RETRIES+1))
done

if [ $RETRIES -eq $MAX_RETRIES ]; then
  echo "Failed to connect to Cassandra after $MAX_RETRIES attempts"
  exit 1
fi

echo "Cassandra is ready! Applying schema..."

# Execute the schema file
$CQLSH_CMD -f "$SCHEMA_FILE"

# Verify the keyspace and table were created
echo "Verifying keyspace and table were created:"
$CQLSH_CMD -e "DESCRIBE KEYSPACE app_keyspace;"

if [ $? -eq 0 ]; then
  echo "Schema setup complete!"
else
  echo "Schema setup failed!"
  exit 1
fi 