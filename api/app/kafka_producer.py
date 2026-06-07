import json
import os
from kafka import KafkaProducer
import time

def get_producer():
    # Retry loop — Kafka takes a few seconds to start
    for i in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            return producer
        except Exception as e:
            print(f"Kafka not ready yet, retrying ({i+1}/10)...")
            time.sleep(3)
    raise Exception("Could not connect to Kafka after 10 retries")

def publish_event(producer, topic: str, event: dict):
    producer.send(topic, value=event)
    producer.flush()
    print(f"Published to {topic}: {event}")