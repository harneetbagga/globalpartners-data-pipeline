import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from utils.athena import run_query

st.set_page_config(page_title="Churn & Retention", layout="wide")
st.title("⚠️ Churn & Retention")
st.markdown("Customer activity profiles and at-risk identification.")
st.markdown("---")

with st.spinner("Loading churn data..."):

    risk_summary = run_query("""
        SELECT overall_risk_tag,
               COUNT(*) as customers,
               ROUND(AVG(CAST(days_since_last_order AS DOUBLE)), 0) as avg_days_inactive,
               ROUND(AVG(CAST(total_revenue AS DOUBLE)), 2) as avg_revenue
        FROM gp_gold.churn_indicators
        GROUP BY overall_risk_tag
        ORDER BY customers DESC
    """)

    spend_trend = run_query("""
        SELECT spend_trend,
               COUNT(*) as customers,
               ROUND(AVG(CAST(pct_spend_change_p1_vs_p2 AS DOUBLE)), 1) as avg_pct_change
        FROM gp_gold.churn_indicators
        WHERE pct_spend_change_p1_vs_p2 IS NOT NULL
        GROUP BY spend_trend
        ORDER BY customers DESC
    """)

    risk_level = run_query("""
        SELECT churn_risk_level,
               COUNT(*) as customers,
               ROUND(AVG(CAST(days_since_last_order AS DOUBLE)), 0) as avg_days
        FROM gp_gold.churn_indicators
        GROUP BY churn_risk_level
        ORDER BY customers DESC
    """)

    at_risk_customers = run_query("""
        SELECT user_id,
               CAST(days_since_last_order AS INT) as days_inactive,
               ROUND(CAST(total_revenue AS DOUBLE), 2) as total_revenue,
               churn_risk_level,
               spend_trend,
               overall_risk_tag,
               CAST(total_orders AS INT) as total_orders,
               ROUND(CAST(avg_days_between_orders AS DOUBLE), 1) as avg_gap_days
        FROM gp_gold.churn_indicators
        WHERE overall_risk_tag IN ('AT_RISK', 'CHURNED')
          AND CAST(total_revenue AS DOUBLE) > 50
        ORDER BY CAST(total_revenue AS DOUBLE) DESC
        LIMIT 25
    """)

# ── KPI row ────────────────────────────────────────────────────────────────
risk_summary['customers'] = risk_summary['customers'].astype(int)
risk_summary['avg_revenue'] = risk_summary['avg_revenue'].astype(float)

col1, col2, col3, col4 = st.columns(4)
color_map = {"ACTIVE": "#4ECDC4", "MONITOR": "#FFB347",
             "AT_RISK": "#FF6B35", "CHURNED": "#FF4444"}

for i, tag in enumerate(["ACTIVE", "MONITOR", "AT_RISK", "CHURNED"]):
    row = risk_summary[risk_summary['overall_risk_tag'] == tag]
    with [col1, col2, col3, col4][i]:
        st.metric(
            tag,
            f"{int(row['customers'].values[0]):,}" if len(row) > 0 else "0",
            f"Avg ${float(row['avg_revenue'].values[0]):,.0f} revenue" if len(row) > 0 else ""
        )

st.markdown("---")

# ── Charts ─────────────────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Overall Risk Distribution")
    fig = px.pie(
        risk_summary,
        values="customers", names="overall_risk_tag",
        color="overall_risk_tag",
        color_discrete_map=color_map,
        hole=0.45,
    )
    fig.update_layout(
        paper_bgcolor="#0A0A0F", font_color="#E8E8F0",
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Spend Trend Distribution")
    spend_trend['customers'] = spend_trend['customers'].astype(int)
    trend_colors = {
        "GROWING": "#4ECDC4", "STABLE": "#FFB347",
        "DECLINING": "#FF6B35", "SEVERELY_DECLINING": "#FF4444",
        "INSUFFICIENT_DATA": "#888888",
    }
    fig = px.bar(
        spend_trend, x="spend_trend", y="customers",
        color="spend_trend",
        color_discrete_map=trend_colors,
        text="customers",
    )
    fig.update_traces(textposition='outside')
    fig.update_layout(
        paper_bgcolor="#0A0A0F", plot_bgcolor="#13131A",
        font_color="#E8E8F0", showlegend=False,
        xaxis_title="Spend Trend", yaxis_title="Customers",
    )
    st.plotly_chart(fig, use_container_width=True)

# ── Churn Risk Level ───────────────────────────────────────────────────────
st.subheader("Churn Risk Level by Days Inactive")
risk_level['customers'] = risk_level['customers'].astype(int)
risk_level['avg_days'] = risk_level['avg_days'].astype(float)

level_colors = {
    "LOW": "#4ECDC4", "MEDIUM": "#FFB347",
    "HIGH": "#FF6B35", "CRITICAL": "#FF4444",
}
fig = px.bar(
    risk_level, x="churn_risk_level", y="customers",
    color="churn_risk_level",
    color_discrete_map=level_colors,
    text="customers",
)
fig.update_traces(textposition='outside')
fig.update_layout(
    paper_bgcolor="#0A0A0F", plot_bgcolor="#13131A",
    font_color="#E8E8F0", showlegend=False,
)
st.plotly_chart(fig, use_container_width=True)

# ── At-risk high value customers ───────────────────────────────────────────
st.subheader("High-Value At-Risk Customers (Revenue > $50)")
at_risk_customers['total_revenue'] = at_risk_customers['total_revenue'].astype(float)
at_risk_customers['days_inactive'] = at_risk_customers['days_inactive'].astype(int)

st.dataframe(
    at_risk_customers.rename(columns={
        'user_id': 'User ID',
        'days_inactive': 'Days Inactive',
        'total_revenue': 'Total Revenue ($)',
        'churn_risk_level': 'Risk Level',
        'spend_trend': 'Spend Trend',
        'overall_risk_tag': 'Risk Tag',
        'total_orders': 'Orders',
        'avg_gap_days': 'Avg Gap (days)',
    }),
    hide_index=True,
    use_container_width=True,
)