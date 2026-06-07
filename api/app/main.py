from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
from datetime import datetime
from postgres import run_postgres_query, run_postgres_write
from kafka_producer import get_producer, publish_event

kafka_producer = get_producer()

POSTGRES_DB_ARGS = dict(
    host=os.environ["POSTGRES_HOST"],
    user=os.environ["POSTGRES_USER"],
    dbname=os.environ["POSTGRES_DB"],
    port=int(os.environ["POSTGRES_PORT"]),
    password=os.environ["POSTGRES_PASSWORD"],
)

app = FastAPI()

class PurchaseEvent(BaseModel):
    product_id: str
    store_id: str
    quantity: int

class RestockEvent(BaseModel):
    product_id: str
    store_id: str
    quantity: int

@app.get("/inventory/{product_id}")
def get_inventory(product_id: str):
    try:
        df = run_postgres_query(
            f"SELECT * FROM inventory WHERE product_id = '{product_id}'",
            **POSTGRES_DB_ARGS,
        )
        return {"product_id": product_id, "result": df.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/purchase")
def purchase(event: PurchaseEvent):
    try:
        df = run_postgres_query(
            f"SELECT quantity FROM inventory WHERE product_id = '{event.product_id}' AND store_id = '{event.store_id}'",
            **POSTGRES_DB_ARGS
        )
        if df.empty:
            raise HTTPException(status_code=404, detail="Product or store not found")
        current_qty = int(df.iloc[0]["quantity"])
        if event.quantity > current_qty:
            raise HTTPException(
                status_code=400,
                detail=f"Insufficient stock. Requested {event.quantity} but only {current_qty} available."
            )
        publish_event(kafka_producer, "inventory-events", {
            "event_type": "purchase",
            **event.model_dump(),
            "timestamp": datetime.now().isoformat()
        })
        return {"status": "ok", "event": event.model_dump(), "timestamp": datetime.now().isoformat()}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/restock")
def restock(event: RestockEvent):
    try:
        publish_event(kafka_producer, "inventory-events", {
            "event_type": "restock",
            **event.model_dump(),
            "timestamp": datetime.now().isoformat()
        })
        return {"status": "ok", "event": event.model_dump(), "timestamp": datetime.now().isoformat()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/health")
def health():
    try:
        run_postgres_query("SELECT 1", **POSTGRES_DB_ARGS)
        return {"status": "healthy", "postgres": "up", "kafka": "up"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unhealthy: {str(e)}")

@app.get("/alerts")
def get_alerts():
    try:
        df = run_postgres_query(
            "SELECT * FROM anomaly_alerts ORDER BY detected_at DESC LIMIT 50",
            **POSTGRES_DB_ARGS,
        )
        return {"alerts": df.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))