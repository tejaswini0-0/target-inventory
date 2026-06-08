## Target Inventory Sync with AI Anomaly Detection
A production-style event-driven microservices system simulating Target's inventory management pipeline, with real-time ML anomaly detection on the event stream.

## Architecture
Purchase and restock events flow from the FastAPI service into Apache Kafka. The inventory consumer reads from Kafka and updates PostgreSQL. Simultaneously, the Isolation Forest anomaly detector watches the same stream and saves alerts to the database. The Streamlit dashboard shows everything live at localhost:8501.

## Services

target_inventory_api — FastAPI app exposing purchase, restock, inventory, and health endpoints
kafka — Apache Kafka message broker for event streaming
zookeeper — Kafka cluster manager
target_postgres — PostgreSQL storing inventory levels and anomaly alerts
target_inventory_consumer — Kafka consumer that updates inventory and triggers auto-restock
target_anomaly_detector — Isolation Forest ML model watching the event stream for anomalies
target_dashboard — Streamlit dashboard showing live inventory and alerts

## Features
- Event-driven architecture: all inventory changes flow through Apache Kafka — the API never writes to the database directly
- Multi-feature streaming anomaly detection using Isolation Forest on three signals simultaneously: purchase quantity, purchase velocity (sliding 60-second window), and time-of-day
- Per store-product anomaly baselines — STORE_NYC and STORE_LA have independent normal patterns, so the same purchase quantity can be normal at one store and anomalous at another
- Anomaly severity classification — alerts are classified as HIGH, MEDIUM, or LOW based on Isolation Forest score, with color coding on the dashboard
- Purchase velocity tracking — counts purchases per product per store in a rolling 60-second window and feeds it as a real-time feature into the ML model
- Auto-restock trigger with 10-minute cooldown — when stock drops below 20 units, automatically restocks by 100 units, but will not fire again for the same product-store within 10 minutes
- Stock validation — purchases above available quantity are rejected at the API level before reaching Kafka
- Full audit trail — every inventory change is logged to inventory_history with timestamp and event type
- Live Streamlit dashboard with control panel, inventory charts, stock history, severity breakdown, and velocity vs quantity anomaly map
- Health check endpoint for service monitoring
- Fully containerized with Docker Compose — one command starts all seven services

## Quick Start
```bash
git clone https://github.com/YOUR_USERNAME/target-inventory.git
cd target-inventory/api
docker compose up --build
```
Open http://localhost:8501 for the dashboard. API runs at http://localhost:80.

## API Endpoints

GET /inventory/{product_id} — current stock levels
POST /purchase — record a purchase event
POST /restock — record a restock event
GET /alerts — get anomaly alerts
GET /health — service health check

## Anomaly Detection
The Isolation Forest model watches every purchase event on the Kafka stream. It builds a rolling history of normal purchase quantities per product and flags outliers — sudden large purchases, potential fraud, or flash-sale patterns. Alerts are saved to the anomaly_alerts table and shown live on the dashboard.

## Tech Stack

Apache Kafka — event streaming
FastAPI — REST API
PostgreSQL — inventory and alerts storage
scikit-learn Isolation Forest — streaming anomaly detection
Streamlit — live dashboard
Docker Compose — local microservices orchestration

## What Makes This Non-Trivial
Rule-based inventory systems decrement on purchase and increment on restock. This project goes further in three ways.
First, the architecture is event-driven. The API publishes to Kafka and returns immediately. The database update happens asynchronously via a consumer. This means any number of downstream services can react to the same event without the API knowing they exist — the same pattern Target uses for scalability.
Second, the anomaly detection is genuinely multi-dimensional. A purchase of 5 units is normal. Twenty purchases of 5 units in 60 seconds is a velocity anomaly. A purchase of 250 units at 3am is both a quantity and time-of-day anomaly. Isolation Forest learns all three dimensions simultaneously from the live event stream without any labeled training data.
Third, the per-store baselines mean the model does not treat all stores the same. A purchase volume that is routine at a high-traffic store like STORE_NYC would be flagged at a smaller store like STORE_LA because each product-store combination maintains its own history.


## Inspired By
Target's Inventory Move team stack: Kafka, microservices, streaming data pipelines, and daily CI/CD deployments.

## Credits
Built on top of the ML Quest 1 Demand Forecasting project by ZazenCodes, licensed under Creative Commons Attribution 4.0 International. The original project provided the FastAPI and Streamlit skeleton which was extended into a full event-driven inventory system with Kafka and ML anomaly detection.

