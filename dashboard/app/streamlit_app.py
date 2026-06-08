import streamlit as st
import pandas as pd
import altair as alt
import os
import requests
from postgres import run_postgres_query

POSTGRES_DB_ARGS = dict(
    host=os.environ["POSTGRES_HOST"],
    user=os.environ["POSTGRES_USER"],
    dbname=os.environ["POSTGRES_DB"],
    port=int(os.environ["POSTGRES_PORT"]),
    password=os.environ["POSTGRES_PASSWORD"],
)

API_URL = "http://target_inventory_api:80"

st.set_page_config(page_title="Target Inventory Dashboard", layout="wide")
st.title("🎯 Target Inventory & Anomaly Detection Dashboard")
st.markdown("Live inventory levels and ML-detected anomalies across all stores.")

if st.button("🔄 Refresh Data"):
    st.rerun()

##################################################################
# CONTROL PANEL
##################################################################

st.header("Control Panel", divider="red")
st.markdown("Simulate purchase and restock events directly from the dashboard.")

inventory_df_for_controls = run_postgres_query(
    "SELECT DISTINCT product_id FROM inventory ORDER BY product_id",
    **POSTGRES_DB_ARGS
)
store_df_for_controls = run_postgres_query(
    "SELECT DISTINCT store_id FROM inventory ORDER BY store_id",
    **POSTGRES_DB_ARGS
)

product_list = inventory_df_for_controls["product_id"].tolist()
store_list = store_df_for_controls["store_id"].tolist()

col1, col2, col3 = st.columns(3)
with col1:
    selected_product = st.selectbox("Product", product_list)
with col2:
    selected_store = st.selectbox("Store", store_list)
with col3:
    selected_quantity = st.slider("Quantity", min_value=1, max_value=500, value=10)

col_buy, col_restock = st.columns(2)
with col_buy:
    if st.button("🛒 Purchase", use_container_width=True):
        try:
            response = requests.post(f"{API_URL}/purchase", json={
                "product_id": selected_product,
                "store_id": selected_store,
                "quantity": selected_quantity
            })
            if response.status_code == 200:
                st.success(f"Purchase of {selected_quantity} units of {selected_product} at {selected_store} sent!")
            else:
                st.error(f"Error: {response.text}")
        except Exception as e:
            st.error(f"Could not reach API: {e}")

with col_restock:
    if st.button("📦 Restock", use_container_width=True):
        try:
            response = requests.post(f"{API_URL}/restock", json={
                "product_id": selected_product,
                "store_id": selected_store,
                "quantity": selected_quantity
            })
            if response.status_code == 200:
                st.success(f"Restock of {selected_quantity} units of {selected_product} at {selected_store} sent!")
            else:
                st.error(f"Error: {response.text}")
        except Exception as e:
            st.error(f"Could not reach API: {e}")

##################################################################
# INVENTORY SECTION
##################################################################

st.header("Current Inventory Levels", divider="red")

inventory_df = run_postgres_query(
    "SELECT * FROM inventory ORDER BY product_id, store_id",
    **POSTGRES_DB_ARGS
)

if inventory_df.empty:
    st.warning("No inventory data found.")
else:
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Products", inventory_df["product_id"].nunique())
    col2.metric("Total Stores", inventory_df["store_id"].nunique())
    col3.metric("Low Stock Items (< 50)", int((inventory_df["quantity"] < 50).sum()))

    st.dataframe(
        inventory_df,
        column_config={
            "product_id": st.column_config.TextColumn("Product"),
            "store_id": st.column_config.TextColumn("Store"),
            "quantity": st.column_config.NumberColumn("Quantity", format="%d"),
            "last_updated": st.column_config.TextColumn("Last Updated"),
        },
        hide_index=True,
        use_container_width=True,
    )

    chart = (
        alt.Chart(inventory_df)
        .mark_bar()
        .encode(
            x=alt.X("product_id:N", title="Product"),
            y=alt.Y("quantity:Q", title="Quantity"),
            color=alt.Color("store_id:N", title="Store"),
            tooltip=["product_id", "store_id", "quantity"],
        )
        .properties(title="Inventory by Product and Store")
    )
    st.altair_chart(chart, use_container_width=True)

##################################################################
# STOCK HISTORY CHART
##################################################################

st.header("Stock History", divider="red")

try:
    history_df = run_postgres_query(
        "SELECT * FROM inventory_history ORDER BY recorded_at DESC LIMIT 200",
        **POSTGRES_DB_ARGS
    )

    if history_df.empty:
        st.info("No history yet — send some purchase or restock events using the control panel above.")
    else:
        selected_history_product = st.selectbox(
            "Select product to view history",
            history_df["product_id"].unique().tolist()
        )

        filtered_history = history_df[history_df["product_id"] == selected_history_product].copy()
        filtered_history["recorded_at"] = pd.to_datetime(filtered_history["recorded_at"])

        history_chart = (
            alt.Chart(filtered_history)
            .mark_point(size=80)
            .encode(
                x=alt.X("recorded_at:T", title="Time"),
                y=alt.Y("quantity:Q", title="Event Quantity"),
                color=alt.Color("event_type:N", title="Event Type"),
                shape=alt.Shape("store_id:N", title="Store"),
                tooltip=["product_id", "store_id", "quantity", "event_type", "recorded_at"],
            )
            .properties(title=f"Event History for {selected_history_product}")
        )
        st.altair_chart(history_chart, use_container_width=True)

except Exception as e:
    st.info("History table not ready yet.")

##################################################################
# ANOMALY ALERTS SECTION
##################################################################

st.header("🚨 Anomaly Alerts", divider="red")

try:
    alerts_df = run_postgres_query(
        "SELECT * FROM anomaly_alerts ORDER BY detected_at DESC LIMIT 50",
        **POSTGRES_DB_ARGS
    )

    if alerts_df.empty:
        st.success("No anomalies detected yet. System is healthy.")
    else:
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Alerts", len(alerts_df))
        col2.metric("High Severity", int((alerts_df["severity"] == "HIGH").sum()))
        col3.metric("Medium Severity", int((alerts_df["severity"] == "MEDIUM").sum()))
        col4.metric("Most Flagged Product", alerts_df["product_id"].mode()[0])

        # Color code severity
        def severity_color(val):
            if val == "HIGH":
                return "background-color: #ffcccc"
            elif val == "MEDIUM":
                return "background-color: #ffe5cc"
            elif val == "LOW":
                return "background-color: #ffffcc"
            return ""

        styled_alerts = alerts_df.style.applymap(severity_color, subset=["severity"])

        st.dataframe(
            styled_alerts,
            column_config={
                "id": None,
                "product_id": st.column_config.TextColumn("Product"),
                "store_id": st.column_config.TextColumn("Store"),
                "quantity": st.column_config.NumberColumn("Suspicious Qty", format="%d"),
                "velocity": st.column_config.NumberColumn("Velocity /min", format="%d"),
                "anomaly_score": st.column_config.NumberColumn("Anomaly Score", format="%.3f"),
                "severity": st.column_config.TextColumn("Severity"),
                "detected_at": st.column_config.TextColumn("Detected At"),
            },
            hide_index=True,
            use_container_width=True,
        )

        # Severity breakdown chart
        severity_counts = alerts_df["severity"].value_counts().reset_index()
        severity_counts.columns = ["severity", "count"]
        severity_order = ["HIGH", "MEDIUM", "LOW"]
        severity_colors = {"HIGH": "#ff4444", "MEDIUM": "#ff8800", "LOW": "#ffcc00"}

        severity_chart = (
            alt.Chart(severity_counts)
            .mark_bar()
            .encode(
                x=alt.X("severity:N", sort=severity_order, title="Severity"),
                y=alt.Y("count:Q", title="Number of Alerts"),
                color=alt.Color("severity:N",
                    scale=alt.Scale(
                        domain=list(severity_colors.keys()),
                        range=list(severity_colors.values())
                    ),
                    legend=None
                ),
                tooltip=["severity", "count"]
            )
            .properties(title="Alerts by Severity")
        )
        st.altair_chart(severity_chart, use_container_width=True)

        # Velocity vs quantity scatter plot
        if len(alerts_df) > 1:
            scatter = (
                alt.Chart(alerts_df)
                .mark_point(size=100)
                .encode(
                    x=alt.X("velocity:Q", title="Purchase Velocity (per min)"),
                    y=alt.Y("quantity:Q", title="Purchase Quantity"),
                    color=alt.Color("severity:N",
                        scale=alt.Scale(
                            domain=list(severity_colors.keys()),
                            range=list(severity_colors.values())
                        )
                    ),
                    tooltip=["product_id", "store_id", "quantity", "velocity", "severity", "anomaly_score"],
                )
                .properties(title="Velocity vs Quantity — Anomaly Map")
            )
            st.altair_chart(scatter, use_container_width=True)

except Exception as e:
    st.info("Anomaly alerts table not yet created — run setup_db.py first.")