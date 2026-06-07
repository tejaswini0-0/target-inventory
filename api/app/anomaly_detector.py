import json
import os
import time
import psycopg2
import numpy as np
from kafka import KafkaConsumer
from collections import defaultdict
from sklearn.ensemble import IsolationForest

# Tracks recent purchase quantities per product to build a rolling window
event_history = defaultdict(list)

def get_db_connection():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        user=os.environ["POSTGRES_USER"],
        dbname=os.environ["POSTGRES_DB"],
        port=int(os.environ["POSTGRES_PORT"]),
        password=os.environ["POSTGRES_PASSWORD"],
    )

def save_alert(product_id, store_id, quantity, anomaly_score):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS anomaly_alerts (
                id SERIAL PRIMARY KEY,
                product_id VARCHAR(100),
                store_id VARCHAR(100),
                quantity INTEGER,
                anomaly_score FLOAT,
                detected_at TIMESTAMP DEFAULT NOW()
            );
        """)
        cur.execute("""
            INSERT INTO anomaly_alerts (product_id, store_id, quantity, anomaly_score)
            VALUES (%s, %s, %s, %s)
        """, (product_id, store_id, quantity, anomaly_score))
        conn.commit()
        cur.close()
        conn.close()
        print(f"ALERT SAVED: {product_id} at {store_id} - quantity {quantity} - score {anomaly_score:.3f}")
    except Exception as e:
        print(f"Error saving alert: {e}")

def detect_anomaly(product_id, quantity):
    history = event_history[product_id]
    history.append(quantity)

    # Need at least 10 events to train the model
    if len(history) < 10:
        print(f"Building history for {product_id}: {len(history)}/10 events seen")
        return False, 0.0

    # Train Isolation Forest on the history
    X = np.array(history).reshape(-1, 1)
    model = IsolationForest(contamination=0.1, random_state=42)
    model.fit(X)

    # Score the latest event
    score = model.decision_function([[quantity]])[0]
    is_anomaly = model.predict([[quantity]])[0] == -1

    return is_anomaly, score

def run_detector():
    # Retry connecting to Kafka
    consumer = None
    for i in range(20):
        try:
            consumer = KafkaConsumer(
                "inventory-events",
                bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                group_id="anomaly-detector-group-v2",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                session_timeout_ms=30000,
            )
            print("Anomaly detector connected to Kafka.")
            break
        except Exception as e:
            print(f"Kafka not ready, retrying ({i+1}/20)... {e}")
            time.sleep(5)

    if consumer is None:
        raise Exception("Could not connect to Kafka")

    print("Anomaly detector started, watching event stream...")

    for message in consumer:
        event = message.value

        # Only watch purchase events for anomalies
        if event["event_type"] != "purchase":
            continue

        product_id = event["product_id"]
        store_id = event["store_id"]
        quantity = event["quantity"]

        print(f"Checking event: {product_id} qty={quantity}")

        is_anomaly, score = detect_anomaly(product_id, quantity)

        if is_anomaly:
            print(f"ANOMALY DETECTED: {product_id} at {store_id} - qty {quantity} - score {score:.3f}")
            save_alert(product_id, store_id, quantity, float(score))
        else:
            print(f"Normal event: {product_id} qty={quantity} score={score:.3f}")

if __name__ == "__main__":
    run_detector()