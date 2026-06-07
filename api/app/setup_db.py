import psycopg2
import os
import time

def setup():
    # Wait for Postgres to be ready
    time.sleep(5)

    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        user=os.environ["POSTGRES_USER"],
        dbname=os.environ["POSTGRES_DB"],
        port=int(os.environ["POSTGRES_PORT"]),
        password=os.environ["POSTGRES_PASSWORD"],
    )
    cur = conn.cursor()

    # Create the inventory table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inventory (
            product_id VARCHAR(100),
            store_id   VARCHAR(100),
            quantity   INTEGER,
            last_updated TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (product_id, store_id)
        );
    """)

    # Seed with some sample Target-style products
    cur.execute("""
        INSERT INTO inventory (product_id, store_id, quantity)
        VALUES
            ('TSHIRT_001', 'STORE_NYC', 200),
            ('TSHIRT_001', 'STORE_LA',  150),
            ('JEANS_002',  'STORE_NYC', 100),
            ('JEANS_002',  'STORE_LA',   80),
            ('SHOES_003',  'STORE_NYC',  60),
            ('SHOES_003',  'STORE_LA',   90)
        ON CONFLICT (product_id, store_id) DO NOTHING;
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Database setup complete.")

if __name__ == "__main__":
    setup()