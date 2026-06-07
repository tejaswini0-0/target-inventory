# Target Inventory Sync with AI Anomaly Detection
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

Event-driven architecture: all inventory changes flow through Kafka
Real-time ML anomaly detection using Isolation Forest (no labeled data needed)
Auto-restock trigger when stock drops below 20 units
Live Streamlit dashboard with inventory charts and anomaly alert feed
Health check endpoint for service monitoring
Fully containerized with Docker Compose

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

## Inspired By
Target's Inventory Move team stack: Kafka, microservices, streaming data pipelines, and daily CI/CD deployments.

## Credits
Built on top of the ML Quest 1 Demand Forecasting project by ZazenCodes, licensed under Creative Commons Attribution 4.0 International. The original project provided the FastAPI and Streamlit skeleton which was extended into a full event-driven inventory system with Kafka and ML anomaly detection.

