import json
import os
import time
import psycopg2
from kafka import KafkaConsumer

RESTOCK_COOLDOWN = 600  # 10 minutes in seconds
LOW_STOCK_THRESHOLD = 20
RESTOCK_AMOUNT = 100

def get_db_connection():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        user=os.environ["POSTGRES_USER"],
        dbname=os.environ["POSTGRES_DB"],
        port=int(os.environ["POSTGRES_PORT"]),
        password=os.environ["POSTGRES_PASSWORD"],
    )

def check_restock_cooldown(product_id, store_id):
    """Returns True if enough time has passed since the last restock."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT last_restock FROM restock_cooldown
            WHERE product_id = %s AND store_id = %s
        """, (product_id, store_id))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row is None:
            return True  # Never restocked before
        last_restock = row[0]
        elapsed = (time.time() - last_restock.timestamp())
        return elapsed > RESTOCK_COOLDOWN
    except Exception as e:
        print(f"Error checking cooldown: {e}")
        return False

def record_restock_time(product_id, store_id):
    """Upsert the last restock timestamp for this product-store."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO restock_cooldown (product_id, store_id, last_restock)
            VALUES (%s, %s, NOW())
            ON CONFLICT (product_id, store_id)
            DO UPDATE SET last_restock = NOW()
        """, (product_id, store_id))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error recording restock time: {e}")

def run_consumer():
    consumer = None
    for i in range(20):
        try:
            consumer = KafkaConsumer(
                "inventory-events",
                bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                group_id="inventory-consumer-group-v3",
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
                # Check current stock before allowing purchase
                cur.execute("""
                    SELECT quantity FROM inventory
                    WHERE product_id = %s AND store_id = %s
                """, (event["product_id"], event["store_id"]))
                row = cur.fetchone()

                if row is None:
                    print(f"Product {event['product_id']} at {event['store_id']} not found.")
                    cur.close()
                    conn.close()
                    continue

                current_qty = row[0]
                if event["quantity"] > current_qty:
                    print(f"REJECTED: {event['product_id']} at {event['store_id']} — requested {event['quantity']} but only {current_qty} in stock.")
                    cur.close()
                    conn.close()
                    continue

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

            # Log to history
            cur.execute("""
                INSERT INTO inventory_history (product_id, store_id, quantity, event_type)
                VALUES (%s, %s, %s, %s)
            """, (event["product_id"], event["store_id"], event["quantity"], event["event_type"]))

            conn.commit()
            cur.close()
            conn.close()
            print(f"Inventory updated for {event['product_id']} at {event['store_id']}")

            # Auto-restock check with cooldown
            conn2 = get_db_connection()
            cur2 = conn2.cursor()
            cur2.execute("""
                SELECT quantity FROM inventory
                WHERE product_id = %s AND store_id = %s
            """, (event["product_id"], event["store_id"]))
            row2 = cur2.fetchone()

            if row2 and row2[0] < LOW_STOCK_THRESHOLD:
                if check_restock_cooldown(event["product_id"], event["store_id"]):
                    cur2.execute("""
                        UPDATE inventory
                        SET quantity = quantity + %s, last_updated = NOW()
                        WHERE product_id = %s AND store_id = %s
                    """, (RESTOCK_AMOUNT, event["product_id"], event["store_id"]))
                    conn2.commit()
                    record_restock_time(event["product_id"], event["store_id"])
                    print(f"Auto-restocked {event['product_id']} at {event['store_id']} by {RESTOCK_AMOUNT} units.")
                else:
                    print(f"Restock cooldown active for {event['product_id']} at {event['store_id']} — skipping.")

            cur2.close()
            conn2.close()

        except Exception as e:
            print(f"Error processing event: {e}")

if __name__ == "__main__":
    run_consumer()