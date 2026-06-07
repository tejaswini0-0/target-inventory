import json
import os
import time
import psycopg2
from kafka import KafkaConsumer

def get_db_connection():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        user=os.environ["POSTGRES_USER"],
        dbname=os.environ["POSTGRES_DB"],
        port=int(os.environ["POSTGRES_PORT"]),
        password=os.environ["POSTGRES_PASSWORD"],
    )


def run_consumer():
    # Retry connecting to Kafka
    consumer = None
    for i in range(20):
        try:
            consumer = KafkaConsumer(
                "inventory-events",
                bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                group_id="inventory-consumer-group-v2",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                session_timeout_ms=30000,
            )
            print("Consumer connected to Kafka successfully.")
            break
        except Exception as e:
            print(f"Kafka not ready yet, retrying ({i+1}/20)... {e}")
            time.sleep(5)

    if consumer is None:
        raise Exception("Could not connect to Kafka after 20 retries")

    print("Consumer started, listening for events...")

    for message in consumer:
        event = message.value
        print(f"Received event: {event}")

        try:
            conn = get_db_connection()
            cur = conn.cursor()

            if event["event_type"] == "purchase":
                cur.execute("""
                    UPDATE inventory
                    SET quantity = quantity - %s,
                        last_updated = NOW()
                    WHERE product_id = %s AND store_id = %s
                """, (event["quantity"], event["product_id"], event["store_id"]))

            elif event["event_type"] == "restock":
                cur.execute("""
                    UPDATE inventory
                    SET quantity = quantity + %s,
                        last_updated = NOW()
                    WHERE product_id = %s AND store_id = %s
                """, (event["quantity"], event["product_id"], event["store_id"]))

            conn.commit()
            cur.close()
            conn.close()
            print(f"Inventory updated for {event['product_id']} at {event['store_id']}")

            # Auto-restock if quantity drops below threshold
            conn2 = get_db_connection()
            cur2 = conn2.cursor()
            cur2.execute("""
                SELECT quantity FROM inventory
                WHERE product_id = %s AND store_id = %s
            """, (event["product_id"], event["store_id"]))
            row = cur2.fetchone()
            if row and row[0] < 20:
                print(f"LOW STOCK: {event['product_id']} at {event['store_id']} = {row[0]} units - auto-restocking")
                cur2.execute("""
                    UPDATE inventory
                    SET quantity = quantity + 100, last_updated = NOW()
                    WHERE product_id = %s AND store_id = %s
                """, (event["product_id"], event["store_id"]))
                conn2.commit()
                print(f"Auto-restock complete.")
            cur2.close()
            conn2.close()

        except Exception as e:
            print(f"Error processing event: {e}")

if __name__ == "__main__":
    run_consumer()