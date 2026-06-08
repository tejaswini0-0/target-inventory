import psycopg2
import os
import time

def setup():
    time.sleep(5)

    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        user=os.environ["POSTGRES_USER"],
        dbname=os.environ["POSTGRES_DB"],
        port=int(os.environ["POSTGRES_PORT"]),
        password=os.environ["POSTGRES_PASSWORD"],
    )
    cur = conn.cursor()

    # Inventory table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inventory (
            product_id VARCHAR(100),
            store_id   VARCHAR(100),
            quantity   INTEGER,
            last_updated TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (product_id, store_id)
        );
    """)

    # Inventory history table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inventory_history (
            id SERIAL PRIMARY KEY,
            product_id VARCHAR(100),
            store_id VARCHAR(100),
            quantity INTEGER,
            event_type VARCHAR(50),
            recorded_at TIMESTAMP DEFAULT NOW()
        );
    """)

    # Anomaly alerts table — with velocity and severity
    cur.execute("""
        CREATE TABLE IF NOT EXISTS anomaly_alerts (
            id SERIAL PRIMARY KEY,
            product_id VARCHAR(100),
            store_id VARCHAR(100),
            quantity INTEGER,
            velocity INTEGER,
            anomaly_score FLOAT,
            severity VARCHAR(20),
            detected_at TIMESTAMP DEFAULT NOW()
        );
    """)

    # Restock cooldown table — tracks last restock per product-store
    cur.execute("""
        CREATE TABLE IF NOT EXISTS restock_cooldown (
            product_id VARCHAR(100),
            store_id VARCHAR(100),
            last_restock TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (product_id, store_id)
        );
    """)

    # Seed inventory
    cur.execute("""
        INSERT INTO inventory (product_id, store_id, quantity)
        VALUES
            ('TSHIRT_001',      'STORE_NYC', 200),
            ('TSHIRT_001',      'STORE_LA',  150),
            ('TSHIRT_001',      'STORE_CHI', 180),
            ('JEANS_002',       'STORE_NYC', 100),
            ('JEANS_002',       'STORE_LA',   80),
            ('JEANS_002',       'STORE_CHI',  90),
            ('SHOES_003',       'STORE_NYC',  60),
            ('SHOES_003',       'STORE_LA',   90),
            ('SHOES_003',       'STORE_CHI',  75),
            ('JACKET_004',      'STORE_NYC', 120),
            ('JACKET_004',      'STORE_LA',   95),
            ('JACKET_004',      'STORE_CHI', 110),
            ('HEADPHONES_005',  'STORE_NYC',  45),
            ('HEADPHONES_005',  'STORE_LA',   30),
            ('HEADPHONES_005',  'STORE_CHI',  50),
            ('LAPTOP_006',      'STORE_NYC',  25),
            ('LAPTOP_006',      'STORE_LA',   20),
            ('LAPTOP_006',      'STORE_CHI',  15),
            ('PILLOW_007',      'STORE_NYC',  80),
            ('PILLOW_007',      'STORE_LA',   60),
            ('PILLOW_007',      'STORE_CHI',  70),
            ('CEREAL_008',      'STORE_NYC', 300),
            ('CEREAL_008',      'STORE_LA',  250),
            ('CEREAL_008',      'STORE_CHI', 280),
            ('DETERGENT_009',   'STORE_NYC', 200),
            ('DETERGENT_009',   'STORE_LA',  175),
            ('DETERGENT_009',   'STORE_CHI', 190),
            ('TOOTHBRUSH_010',  'STORE_NYC', 400),
            ('TOOTHBRUSH_010',  'STORE_LA',  350),
            ('TOOTHBRUSH_010',  'STORE_CHI', 375)
        ON CONFLICT (product_id, store_id) DO NOTHING;
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Database setup complete.")

if __name__ == "__main__":
    setup()