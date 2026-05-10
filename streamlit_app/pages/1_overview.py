import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from utils.athena import run_query

st.set_page_config(page_title="Overview", layout="wide")
st.title("📊 Overview")
st.markdown("Key performance indicators across the entire GlobalPartners dataset.")
st.markdown("---")

# ── KPI Cards ──────────────────────────────────────────────────────────────
with st.spinner("Loading KPIs..."):

    total_customers = run_query("""
        SELECT COUNT(DISTINCT user_id) as val
        FROM gp_gold.clv
    """)

    total_revenue = run_query("""
        SELECT ROUND(SUM(total_net_revenue), 0) as val
        FROM gp_gold.clv
    """)

    clv_bands = run_query("""
        SELECT clv_band,
               COUNT(*) as customers,
               ROUND(AVG(CAST(total_net_revenue AS DOUBLE)), 2) as avg_revenue
        FROM gp_gold.clv
        GROUP BY clv_band
        ORDER BY clv_band
    """)

    rfm_segments = run_query("""
        SELECT segment, COUNT(*) as customers
        FROM gp_gold.rfm
        GROUP BY segment
        ORDER BY customers DESC
    """)

    churn_summary = run_query("""
        SELECT overall_risk_tag, COUNT(*) as customers
        FROM gp_gold.churn_indicators
        GROUP BY overall_risk_tag
        ORDER BY customers DESC
    """)

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Customers", f"{int(total_customers['val'].iloc[0]):,}")

with col2:
    rev = float(total_revenue['val'].iloc[0])
    st.metric("Total Net Revenue", f"${rev:,.0f}")

with col3:
    high = clv_bands[clv_bands['clv_band'] == 'HIGH']['customers'].values
    st.metric("HIGH CLV Customers", f"{int(high[0]):,}" if len(high) > 0 else "N/A")

with col4:
    churned = churn_summary[churn_summary['overall_risk_tag'] == 'CHURNED']['customers'].values
    st.metric("Churned Customers", f"{int(churned[0]):,}" if len(churned) > 0 else "N/A")

st.markdown("---")

# ── Charts ─────────────────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("CLV Band Distribution")
    clv_bands['customers'] = clv_bands['customers'].astype(int)
    fig = px.pie(
        clv_bands,
        values  = "customers",
        names   = "clv_band",
        color_discrete_sequence = ["#FF6B35", "#FFB347", "#4ECDC4"],
        hole    = 0.4,
    )
    fig.update_layout(
        paper_bgcolor = "#0A0A0F",
        plot_bgcolor  = "#0A0A0F",
        font_color    = "#E8E8F0",
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("RFM Segment Distribution")
    rfm_segments['customers'] = rfm_segments['customers'].astype(int)
    fig = px.bar(
        rfm_segments,
        x     = "customers",
        y     = "segment",
        orientation = "h",
        color = "customers",
        color_continuous_scale = "Oranges",
    )
    fig.update_layout(
        paper_bgcolor = "#0A0A0F",
        plot_bgcolor  = "#13131A",
        font_color    = "#E8E8F0",
        showlegend    = False,
        yaxis_title   = "",
        xaxis_title   = "Customers",
    )
    st.plotly_chart(fig, use_container_width=True)

# ── Churn Risk ─────────────────────────────────────────────────────────────
st.subheader("Customer Risk Distribution")
churn_summary['customers'] = churn_summary['customers'].astype(int)

color_map = {
    "ACTIVE":  "#4ECDC4",
    "MONITOR": "#FFB347",
    "AT_RISK": "#FF6B35",
    "CHURNED": "#FF4444",
}

fig = px.bar(
    churn_summary,
    x     = "overall_risk_tag",
    y     = "customers",
    color = "overall_risk_tag",
    color_discrete_map = color_map,
)
fig.update_layout(
    paper_bgcolor = "#0A0A0F",
    plot_bgcolor  = "#13131A",
    font_color    = "#E8E8F0",
    showlegend    = False,
    xaxis_title   = "Risk Tag",
    yaxis_title   = "Customers",
)
st.plotly_chart(fig, use_container_width=True)