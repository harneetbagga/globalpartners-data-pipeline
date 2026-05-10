import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from utils.athena import run_query

st.set_page_config(page_title="Customer Analytics", layout="wide")
st.title("👥 Customer Analytics")
st.markdown("CLV bands, RFM segments, and top customers by lifetime value.")
st.markdown("---")

# ── Load data ──────────────────────────────────────────────────────────────
with st.spinner("Loading customer data..."):

    clv_data = run_query("""
        SELECT clv_band,
               COUNT(*) as customers,
               ROUND(AVG(CAST(total_net_revenue AS DOUBLE)), 2) as avg_revenue,
               ROUND(AVG(CAST(total_orders AS DOUBLE)), 1) as avg_orders,
               ROUND(AVG(CAST(days_as_customer AS DOUBLE)), 0) as avg_lifespan_days
        FROM gp_gold.clv
        GROUP BY clv_band
        ORDER BY clv_band
    """)

    top_customers = run_query("""
        SELECT user_id,
               ROUND(CAST(total_net_revenue AS DOUBLE), 2) as total_net_revenue,
               CAST(total_orders AS INT) as total_orders,
               clv_band,
               CAST(is_loyalty_member AS VARCHAR) as is_loyalty,
               CAST(days_since_last_order AS INT) as days_since_last_order,
               CAST(unique_restaurants_visited AS INT) as restaurants_visited
        FROM gp_gold.clv
        ORDER BY CAST(total_net_revenue AS DOUBLE) DESC
        LIMIT 20
    """)

    rfm_data = run_query("""
        SELECT segment,
               COUNT(*) as customers,
               ROUND(AVG(CAST(monetary AS DOUBLE)), 2) as avg_spend,
               ROUND(AVG(CAST(frequency AS DOUBLE)), 1) as avg_orders,
               ROUND(AVG(CAST(recency_days AS DOUBLE)), 0) as avg_recency_days
        FROM gp_gold.rfm
        GROUP BY segment
        ORDER BY customers DESC
    """)

    loyalty_clv = run_query("""
        SELECT CAST(is_loyalty_member AS VARCHAR) as is_loyalty,
               COUNT(*) as customers,
               ROUND(AVG(CAST(total_net_revenue AS DOUBLE)), 2) as avg_clv,
               ROUND(AVG(CAST(total_orders AS DOUBLE)), 1) as avg_orders
        FROM gp_gold.clv
        GROUP BY is_loyalty_member
    """)

# ── KPI row ────────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

clv_data['customers'] = clv_data['customers'].astype(int)
clv_data['avg_revenue'] = clv_data['avg_revenue'].astype(float)

high = clv_data[clv_data['clv_band'] == 'HIGH']
med  = clv_data[clv_data['clv_band'] == 'MEDIUM']
low  = clv_data[clv_data['clv_band'] == 'LOW']

with col1:
    st.metric("HIGH CLV Customers",
              f"{high['customers'].values[0]:,}" if len(high) > 0 else "N/A",
              f"Avg ${high['avg_revenue'].values[0]:,.0f}" if len(high) > 0 else "")
with col2:
    st.metric("MEDIUM CLV Customers",
              f"{med['customers'].values[0]:,}" if len(med) > 0 else "N/A",
              f"Avg ${med['avg_revenue'].values[0]:,.0f}" if len(med) > 0 else "")
with col3:
    st.metric("LOW CLV Customers",
              f"{low['customers'].values[0]:,}" if len(low) > 0 else "N/A",
              f"Avg ${low['avg_revenue'].values[0]:,.0f}" if len(low) > 0 else "")
with col4:
    total = clv_data['customers'].sum()
    st.metric("Total Customers", f"{total:,}")

st.markdown("---")

# ── Charts row 1 ───────────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("CLV Band — Avg Revenue vs Customer Count")
    fig = go.Figure()
    colors = {"HIGH": "#FF6B35", "MEDIUM": "#FFB347", "LOW": "#4ECDC4"}
    for _, row in clv_data.iterrows():
        fig.add_trace(go.Bar(
            x=[row['clv_band']],
            y=[row['avg_revenue']],
            name=row['clv_band'],
            marker_color=colors.get(row['clv_band'], "#888"),
        ))
    fig.update_layout(
        paper_bgcolor="#0A0A0F", plot_bgcolor="#13131A",
        font_color="#E8E8F0", showlegend=False,
        xaxis_title="CLV Band", yaxis_title="Avg Net Revenue ($)",
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Loyalty vs Non-Loyalty CLV")
    loyalty_clv['avg_clv'] = loyalty_clv['avg_clv'].astype(float)
    loyalty_clv['label'] = loyalty_clv['is_loyalty'].map(
        {'true': 'Loyalty Member', 'false': 'Non-Loyalty'}
    )
    fig = px.bar(
        loyalty_clv,
        x="label", y="avg_clv",
        color="label",
        color_discrete_map={
            "Loyalty Member": "#FF6B35",
            "Non-Loyalty": "#4ECDC4"
        },
        text="avg_clv",
    )
    fig.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
    fig.update_layout(
        paper_bgcolor="#0A0A0F", plot_bgcolor="#13131A",
        font_color="#E8E8F0", showlegend=False,
        xaxis_title="", yaxis_title="Avg CLV ($)",
    )
    st.plotly_chart(fig, use_container_width=True)

# ── RFM Segments ───────────────────────────────────────────────────────────
st.subheader("RFM Segment Distribution")
rfm_data['customers'] = rfm_data['customers'].astype(int)
rfm_data['avg_spend'] = rfm_data['avg_spend'].astype(float)

col1, col2 = st.columns([2, 1])

with col1:
    fig = px.bar(
        rfm_data, x="customers", y="segment",
        orientation="h",
        color="avg_spend",
        color_continuous_scale="Oranges",
        text="customers",
    )
    fig.update_traces(textposition='outside')
    fig.update_layout(
        paper_bgcolor="#0A0A0F", plot_bgcolor="#13131A",
        font_color="#E8E8F0", yaxis_title="",
        xaxis_title="Number of Customers",
        coloraxis_showscale=False,
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.dataframe(
        rfm_data[['segment', 'customers', 'avg_spend', 'avg_orders']].rename(columns={
            'segment': 'Segment',
            'customers': 'Customers',
            'avg_spend': 'Avg Spend ($)',
            'avg_orders': 'Avg Orders',
        }),
        hide_index=True,
        use_container_width=True,
    )

# ── Top 20 Customers ───────────────────────────────────────────────────────
st.subheader("Top 20 Customers by Lifetime Value")
top_customers['total_net_revenue'] = top_customers['total_net_revenue'].astype(float)
st.dataframe(
    top_customers.rename(columns={
        'user_id': 'User ID',
        'total_net_revenue': 'Net Revenue ($)',
        'total_orders': 'Orders',
        'clv_band': 'CLV Band',
        'is_loyalty': 'Loyalty',
        'days_since_last_order': 'Days Inactive',
        'restaurants_visited': 'Restaurants',
    }),
    hide_index=True,
    use_container_width=True,
)