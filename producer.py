"""
AtmoSync - IoT Sensor Simulator (Kafka Producer)
--------------------------------------------------
Ye script fake shipping container sensors ka data generate karta hai
(temperature, humidity, vibration) aur Kafka topic 'container-telemetry'
mein bhejta hai, jaise real IoT sensors bhejte.

Run karne se pehle install karo:
    pip install kafka-python faker

Run karo:
    python producer.py
"""

import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

# ---- Config ----
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "container-telemetry"

# Simulate 5 shipping containers, each carrying a commodity
CONTAINERS = [
    {"container_id": "CONT-A", "commodity": "avocado", "base_temp": 5.0, "base_humidity": 85},
    {"container_id": "CONT-B", "commodity": "banana", "base_temp": 13.0, "base_humidity": 90},
    {"container_id": "CONT-C", "commodity": "grapes", "base_temp": 1.0, "base_humidity": 92},
    {"container_id": "CONT-D", "commodity": "mango", "base_temp": 10.0, "base_humidity": 88},
    {"container_id": "CONT-E", "commodity": "strawberry", "base_temp": 0.5, "base_humidity": 90},
]


def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def generate_reading(container):
    """
    Normally readings drift slightly around the ideal storage temperature.
    Occasionally (10% chance) we simulate a 'micro-climate spike' -
    this is the core scenario from the project: a sudden temperature/humidity
    drift that increases spoilage risk.
    """
    is_anomaly = random.random() < 0.10

    if is_anomaly:
        temp_drift = random.uniform(4.0, 9.0)   # sudden spike
        humidity_drift = random.uniform(5.0, 15.0)
    else:
        temp_drift = random.uniform(-0.5, 0.5)  # normal small fluctuation
        humidity_drift = random.uniform(-2.0, 2.0)

    reading = {
        "container_id": container["container_id"],
        "commodity": container["commodity"],
        "temperature_c": round(container["base_temp"] + temp_drift, 2),
        "humidity_pct": round(container["base_humidity"] + humidity_drift, 2),
        "vibration_g": round(random.uniform(0.01, 0.05) + (0.1 if is_anomaly else 0), 3),
        "is_anomaly_injected": is_anomaly,  # helpful for you to verify pipeline later
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return reading


def main():
    producer = create_producer()
    print(f"Connected to Kafka at {KAFKA_BROKER}")
    print(f"Streaming fake sensor data to topic '{TOPIC_NAME}'... (Ctrl+C to stop)\n")

    try:
        while True:
            for container in CONTAINERS:
                reading = generate_reading(container)
                producer.send(TOPIC_NAME, value=reading)
                flag = " <-- ANOMALY" if reading["is_anomaly_injected"] else ""
                print(f"[{reading['timestamp']}] {reading['container_id']} "
                      f"temp={reading['temperature_c']}C humidity={reading['humidity_pct']}%{flag}")
            producer.flush()
            time.sleep(2)  # send a new batch of readings every 2 seconds
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
