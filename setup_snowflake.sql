-- AtmoSync: Snowflake setup script
-- Run this in a Snowflake Worksheet (copy-paste all of it, then Run All)

-- 1. Create a database for this project
CREATE DATABASE IF NOT EXISTS ATMOSYNC_DB;

-- 2. Use it
USE DATABASE ATMOSYNC_DB;

-- 3. Create a schema (like a folder inside the database)
CREATE SCHEMA IF NOT EXISTS RAW;

USE SCHEMA RAW;

-- 4. Create a table to hold the raw sensor readings coming from Kafka
CREATE TABLE IF NOT EXISTS CONTAINER_TELEMETRY (
    container_id STRING,
    commodity STRING,
    temperature_c FLOAT,
    humidity_pct FLOAT,
    vibration_g FLOAT,
    is_anomaly_injected BOOLEAN,
    event_timestamp TIMESTAMP_TZ,
    loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- 5. Quick check - table should be empty right now
SELECT * FROM CONTAINER_TELEMETRY;

-- 6. Also note down your warehouse name (usually "COMPUTE_WH" by default)
SHOW WAREHOUSES;
