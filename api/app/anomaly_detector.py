import json
import os
import time
import psycopg2
import numpy as np
from kafka import KafkaConsumer
from collections import defaultdict
from datetime import datetime, timezone
from sklearn.ensemble import IsolationForest

# Tracks recent purchase quantities per product-store combination
# Key: (product_id, store_id), Value: list of quantities
quantity_history = defaultdict(list)

# Tracks recent purchase timestamps per product-store combination
# Key: (product_id, store_id), Value: list of UTC timestamps
velocity_history = defaultdict(list)

# Tracks last restock timestamp per product-store combination
# Key: (product_id, store_id), Value: datetime
last_restock_time = {}

MIN_HISTORY = 10

VELOCITY_WINDOW = 60

RESTOCK_COOLDOWN = 600

def get_db_connection():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        user=os.environ["POSTGRES_USER"],
        dbname=os.environ["POSTGRES_DB"],
        port=int(os.environ["POSTGRES_PORT"]),
        password=os.environ["POSTGRES_PASSWORD"],
    )

def get_severity(score):
    """Classify anomaly severity based on Isolation Forest score."""
    if score < -0.3:
        return "HIGH"
    elif score < -0.15:
        return "MEDIUM"
    else:
        return "LOW"

def get_velocity(product_id, store_id, now):
    """Count purchases in the last VELOCITY_WINDOW seconds."""
    key = (product_id, store_id)
    history = velocity_history[key]
    cutoff = now - VELOCITY_WINDOW
    velocity_history[key] = [t for t in history if t > cutoff]
    return len(velocity_history[key])

def get_time_of_day_feature(now):
    """
    Convert current hour into a cyclical feature.
    3am and 11pm are close on a 24hr cycle — sin/cos encoding captures that.
    """
    hour = now % 86400 / 3600
    return np.sin(2 * np.pi * hour / 24), np.cos(2 * np.pi * hour / 24)

def save_alert(product_id, store_id, quantity, velocity, anomaly_score, severity):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO anomaly_alerts
                (product_id, store_id, quantity, velocity, anomaly_score, severity)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (product_id, store_id, quantity, velocity, float(anomaly_score), severity))
        conn.commit()
        cur.close()
        conn.close()
        print(f"ALERT SAVED [{severity}]: {product_id} at {store_id} — qty={quantity} vel={velocity}/min score={anomaly_score:.3f}")
    except Exception as e:
        print(f"Error saving alert: {e}")

def detect_anomaly(product_id, store_id, quantity, velocity, now):
    key = (product_id, store_id)
    history = quantity_history[key]
    history.append(quantity)

    if len(history) < MIN_HISTORY:
        print(f"Building history for {product_id} at {store_id}: {len(history)}/{MIN_HISTORY}")
        return False, 0.0

    # Build feature matrix: [quantity, velocity, sin_hour, cos_hour]
    sin_h, cos_h = get_time_of_day_feature(now)
    current_features = [quantity, velocity, sin_h, cos_h]

    X = []
    for q in history[:-1]:
        X.append([q, 1.0, 0.0, 1.0])

    X.append(current_features)
    X = np.array(X)

    model = IsolationForest(contamination=0.1, random_state=42)
    model.fit(X)

    score = model.decision_function([current_features])[0]
    is_anomaly = model.predict([current_features])[0] == -1

    return is_anomaly, score

def run_detector():
    consumer = None
    for i in range(20):
        try:
            consumer = KafkaConsumer(
                "inventory-events",
                bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                group_id="anomaly-detector-group-v3",
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

    print("Anomaly detector started — multi-feature streaming detection active.")

    for message in consumer:
        event = message.value

        if event["event_type"] != "purchase":
            continue

        product_id = event["product_id"]
        store_id = event["store_id"]
        quantity = event["quantity"]
        now = time.time()

        velocity_history[(product_id, store_id)].append(now)

        velocity = get_velocity(product_id, store_id, now)

        print(f"Checking: {product_id} at {store_id} — qty={quantity} vel={velocity}/min")

        is_anomaly, score = detect_anomaly(product_id, store_id, quantity, velocity, now)

        if is_anomaly:
            severity = get_severity(score)
            print(f"ANOMALY [{severity}]: {product_id} at {store_id} score={score:.3f}")
            save_alert(product_id, store_id, quantity, velocity, score, severity)
        else:
            print(f"Normal: {product_id} at {store_id} score={score:.3f}")

if __name__ == "__main__":
    run_detector()