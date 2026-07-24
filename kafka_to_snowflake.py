"""
AtmoSync - Kafka to Snowflake Loader
--------------------------------------
Ye script Kafka topic 'container-telemetry' se live data padhta hai
aur seedha Snowflake ke CONTAINER_TELEMETRY table mein insert karta hai.

Install karo pehle (agar nahi kiya):
    pip install kafka-python snowflake-connector-python

SETUP: Apna Snowflake password environment variable se set karo
(terminal mein chalao, run karne se pehle):

    Windows (PowerShell):  $env:SNOWFLAKE_PASSWORD = "apna_password"
    Windows (CMD):         set SNOWFLAKE_PASSWORD=apna_password
    Mac/Linux:              export SNOWFLAKE_PASSWORD="apna_password"

Run karo:
    python kafka_to_snowflake.py
"""

import json
import os
from kafka import KafkaConsumer
import snowflake.connector

# ============================================
# CONFIG - apni details yaha bharo
# ============================================
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "container-telemetry"

SNOWFLAKE_ACCOUNT = "xjlgegz-um28221"      # tumhara account identifier
SNOWFLAKE_USER = "Abinashpatra038"          # tumhara username
SNOWFLAKE_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD")  # environment variable se aata hai (safe)
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DATABASE = "ATMOSYNC_DB"
SNOWFLAKE_SCHEMA = "RAW"
SNOWFLAKE_ROLE = "ACCOUNTADMIN"

if not SNOWFLAKE_PASSWORD:
    raise SystemExit(
        "ERROR: SNOWFLAKE_PASSWORD environment variable set nahi hai.\n"
        "Terminal mein pehle ye chalao:\n"
        '  Windows (PowerShell): $env:SNOWFLAKE_PASSWORD = "apna_password"\n'
        '  Windows (CMD):        set SNOWFLAKE_PASSWORD=apna_password'
    )


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )


def get_kafka_consumer():
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="atmosync-snowflake-loader",
    )


INSERT_SQL = """
    INSERT INTO ATMOSYNC_DB.RAW.CONTAINER_TELEMETRY
    (container_id, commodity, temperature_c, humidity_pct, vibration_g,
     is_anomaly_injected, event_timestamp)
    VALUES (%(container_id)s, %(commodity)s, %(temperature_c)s, %(humidity_pct)s,
            %(vibration_g)s, %(is_anomaly_injected)s, %(timestamp)s)
"""


def main():
    print("Connecting to Snowflake...")
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    print("Connected to Snowflake successfully!")

    print("Connecting to Kafka...")
    consumer = get_kafka_consumer()
    print(f"Listening to topic '{TOPIC_NAME}'... (Ctrl+C to stop)\n")

    count = 0
    try:
        for message in consumer:
            reading = message.value
            cursor.execute(INSERT_SQL, reading)
            count += 1
            print(f"[{count}] Inserted -> {reading['container_id']} "
                  f"temp={reading['temperature_c']}C humidity={reading['humidity_pct']}%")

            if count % 10 == 0:
                conn.commit()
                print(f"    --> Committed {count} records to Snowflake so far.\n")

    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Final commit done. Total records loaded: {count}")


if __name__ == "__main__":
    main()
