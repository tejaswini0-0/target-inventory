import streamlit as st
import pandas as pd
import altair as alt
import os
from postgres import run_postgres_query

POSTGRES_DB_ARGS = dict(
    host=os.environ["POSTGRES_HOST"],
    user=os.environ["POSTGRES_USER"],
    dbname=os.environ["POSTGRES_DB"],
    port=int(os.environ["POSTGRES_PORT"]),
    password=os.environ["POSTGRES_PASSWORD"],
)

st.set_page_config(page_title="Target Inventory Dashboard", layout="wide")
st.title("🎯 Target Inventory & Anomaly Detection Dashboard")
st.markdown("Live inventory levels and ML-detected anomalies across all stores.")

# Auto-refresh every 10 seconds
st.markdown(
    "<meta http-equiv='refresh' content='10'>",
    unsafe_allow_html=True
)

##################################################################
# INVENTORY SECTION
##################################################################

st.header("Current Inventory Levels", divider="red")

inventory_df = run_postgres_query("SELECT * FROM inventory ORDER BY product_id, store_id", **POSTGRES_DB_ARGS)

if inventory_df.empty:
    st.warning("No inventory data found.")
else:
    # Summary metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Products", inventory_df["product_id"].nunique())
    col2.metric("Total Stores", inventory_df["store_id"].nunique())
    col3.metric("Low Stock Items (< 50)", int((inventory_df["quantity"] < 50).sum()))

    # Inventory table with color coding
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

    # Bar chart of inventory by product and store
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
        st.error(f"{len(alerts_df)} anomalies detected!")

        col1, col2 = st.columns(2)
        col1.metric("Total Alerts", len(alerts_df))
        col2.metric("Most Flagged Product", alerts_df["product_id"].mode()[0])

        st.dataframe(
            alerts_df,
            column_config={
                "id": None,
                "product_id": st.column_config.TextColumn("Product"),
                "store_id": st.column_config.TextColumn("Store"),
                "quantity": st.column_config.NumberColumn("Suspicious Quantity", format="%d"),
                "anomaly_score": st.column_config.NumberColumn("Anomaly Score", format="%.3f"),
                "detected_at": st.column_config.TextColumn("Detected At"),
            },
            hide_index=True,
            use_container_width=True,
        )

        # Chart of anomalous quantities over time
        if len(alerts_df) > 1:
            alerts_chart = (
                alt.Chart(alerts_df)
                .mark_point(size=100, color="red")
                .encode(
                    x=alt.X("detected_at:T", title="Time"),
                    y=alt.Y("quantity:Q", title="Flagged Quantity"),
                    tooltip=["product_id", "store_id", "quantity", "anomaly_score"],
                )
                .properties(title="Anomaly Timeline")
            )
            st.altair_chart(alerts_chart, use_container_width=True)

except Exception as e:
    st.info("Anomaly alerts table not yet created — send some purchase events first.")