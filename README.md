# AtmoSync — Micro-Climate Arbitrage Analytics

Real-time IoT streaming pipeline that detects shipping-container spoilage risk before goods degrade.

## Problem

Traditional supply-chain analytics rely on standard transit times and macro-weather forecasts.
They miss sudden, hyper-local micro-climate shifts inside a container (e.g. a temperature spike)
that quietly degrade perishable goods before they reach market.

## Architecture

```
Python IoT Simulator → Apache Kafka → Snowflake → dbt → Apache Superset
   (sensor data)        (streaming)   (storage)  (risk logic)  (dashboard)
```

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Apache Kafka | High-throughput streaming of container telemetry |
| Storage | Snowflake | Cloud data warehouse for raw + transformed data |
| Transformation | dbt | SQL models that classify spoilage risk (LOW/MEDIUM/HIGH) |
| Visualization | Apache Superset | Live BI dashboard connected to Snowflake |

## Repository structure

```
├── docker-compose.yml          # Spins up Kafka, Zookeeper, Kafka UI, Superset
├── producer.py                 # Simulates IoT sensors, streams to Kafka
├── kafka_to_snowflake.py       # Consumes Kafka topic, loads into Snowflake
├── setup_snowflake.sql         # Creates the Snowflake database/schema/table
└── atmosync_dbt/
    └── models/
        ├── staging/
        │   ├── sources.yml
        │   └── stg_container_telemetry.sql
        └── marts/
            └── spoilage_risk.sql
```

## How it works

1. `producer.py` simulates 5 shipping containers (avocado, banana, grapes, mango,
   strawberry) and streams temperature/humidity/vibration readings to a Kafka topic
   every 2 seconds, occasionally injecting a micro-climate anomaly.
2. `kafka_to_snowflake.py` consumes that Kafka topic and inserts every reading into
   a Snowflake table (`CONTAINER_TELEMETRY`).
3. dbt models transform the raw data:
   - `stg_container_telemetry` cleans the raw table.
   - `spoilage_risk` classifies each reading as `LOW` / `MEDIUM` / `HIGH` risk based
     on temperature and humidity thresholds.
4. Apache Superset connects directly to Snowflake and visualizes the risk
   distribution on a live dashboard.

## Running it locally

```bash
# 1. Start Kafka + Superset
docker compose up -d

# 2. Create the Snowflake objects
#    (run setup_snowflake.sql in a Snowflake worksheet)

# 3. Start the sensor simulator
python producer.py

# 4. In a second terminal, set your Snowflake password and start the loader
export SNOWFLAKE_PASSWORD="your_password_here"   # PowerShell: $env:SNOWFLAKE_PASSWORD="..."
python kafka_to_snowflake.py

# 5. Build the dbt models
cd atmosync_dbt
dbt run
```

## Status

- [x] Kafka streaming ingestion
- [x] Snowflake storage pipeline
- [x] dbt spoilage-risk models
- [x] Superset live dashboard
- [ ] Automated scheduling
- [ ] Slack/email alerts on HIGH risk
