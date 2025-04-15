-- Cassandra Schema for Kafka-Cassandra Stream Processor
-- This script creates the keyspace and table required for the application

-- Create Keyspace
-- Change the replication strategy and factor based on your deployment environment
-- SimpleStrategy is for single datacenter deployments
-- NetworkTopologyStrategy should be used for production multi-datacenter deployments
CREATE KEYSPACE IF NOT EXISTS app_keyspace
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 3
};

-- Create Table
-- The table stores the data received from Kafka messages
CREATE TABLE IF NOT EXISTS app_keyspace.data_table (
  id text PRIMARY KEY,
  payload_data text,
  timestamp bigint
);

-- Optionally add indices if you need to query by fields other than the primary key
-- CREATE INDEX IF NOT EXISTS data_table_timestamp_idx ON app_keyspace.data_table (timestamp); 