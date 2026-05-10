import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from utils.athena import run_query

st.set_page_config(page_title="Sales Trends", layout="wide")
st.title("📈 Sales Trends")
st.markdown("Daily, weekly, and monthly revenue patterns with holiday enrichment.")
st.markdown("---")

with st.spinner("Loading sales data..."):

    monthly = run_query("""
        SELECT year, month,
               SUM(CAST(gross_revenue AS DOUBLE)) as total_revenue,
               SUM(CAST(total_orders AS BIGINT)) as total_orders,
               SUM(CAST(unique_customers AS BIGINT)) as unique_customers
        FROM gp_gold.sales_trends_monthly
        GROUP BY year, month
        ORDER BY year, month
    """)

    day_of_week = run_query("""
        SELECT day_of_week,
               ROUND(SUM(CAST(gross_revenue AS DOUBLE)), 2) as total_revenue,
               SUM(CAST(total_orders AS BIGINT)) as total_orders,
               ROUND(AVG(CAST(avg_order_value AS DOUBLE)), 2) as avg_order_value
        FROM gp_gold.sales_trends_daily
        WHERE day_of_week IS NOT NULL
        GROUP BY day_of_week
        ORDER BY total_revenue DESC
    """)

    holiday = run_query("""
        SELECT CAST(is_holiday AS VARCHAR) as is_holiday,
               COUNT(*) as days,
               ROUND(SUM(CAST(gross_revenue AS DOUBLE)), 2) as total_revenue,
               ROUND(AVG(CAST(gross_revenue AS DOUBLE)), 2) as avg_daily_revenue,
               SUM(CAST(total_orders AS BIGINT)) as total_orders
        FROM gp_gold.sales_trends_daily
        GROUP BY is_holiday
    """)

    top_days = run_query("""
        SELECT CAST(order_date AS VARCHAR) as order_date,
               restaurant_id,
               ROUND(SUM(CAST(gross_revenue AS DOUBLE)), 2) as revenue,
               SUM(CAST(total_orders AS BIGINT)) as orders,
               CAST(is_holiday AS VARCHAR) as is_holiday,
               day_of_week
        FROM gp_gold.sales_trends_daily
        GROUP BY order_date, restaurant_id, is_holiday, day_of_week
        ORDER BY revenue DESC
        LIMIT 10
    """)

# ── KPI row ────────────────────────────────────────────────────────────────
monthly['total_revenue'] = monthly['total_revenue'].astype(float)
monthly['total_orders'] = monthly['total_orders'].astype(int)

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Revenue", f"${monthly['total_revenue'].sum():,.0f}")
with col2:
    st.metric("Total Orders", f"{monthly['total_orders'].sum():,}")
with col3:
    st.metric("Months of Data", f"{len(monthly):,}")

st.markdown("---")

# ── Monthly revenue trend ──────────────────────────────────────────────────
st.subheader("Monthly Revenue Trend")
monthly['period'] = monthly['year'] + '-' + monthly['month'].str.zfill(2)
monthly = monthly.sort_values('period')

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=monthly['period'],
    y=monthly['total_revenue'],
    mode='lines+markers',
    line=dict(color='#FF6B35', width=2),
    marker=dict(size=4),
    fill='tozeroy',
    fillcolor='rgba(255, 107, 53, 0.1)',
    name='Revenue',
))
fig.update_layout(
    paper_bgcolor="#0A0A0F", plot_bgcolor="#13131A",
    font_color="#E8E8F0",
    xaxis_title="Month", yaxis_title="Revenue ($)",
    xaxis=dict(tickangle=45),
    height=400,
)
st.plotly_chart(fig, use_container_width=True)

# ── Day of week + Holiday ──────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Revenue by Day of Week")
    day_of_week['total_revenue'] = day_of_week['total_revenue'].astype(float)
    fig = px.bar(
        day_of_week, x="day_of_week", y="total_revenue",
        color="total_revenue",
        color_continuous_scale="Oranges",
        text="total_revenue",
    )
    fig.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
    fig.update_layout(
        paper_bgcolor="#0A0A0F", plot_bgcolor="#13131A",
        font_color="#E8E8F0", showlegend=False,
        coloraxis_showscale=False,
        xaxis_title="Day of Week", yaxis_title="Total Revenue ($)",
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Holiday vs Non-Holiday Revenue")
    holiday['total_revenue'] = holiday['total_revenue'].astype(float)
    holiday['avg_daily_revenue'] = holiday['avg_daily_revenue'].astype(float)
    holiday['label'] = holiday['is_holiday'].map(
        {'true': 'Holiday', 'false': 'Regular Day'}
    )
    fig = px.bar(
        holiday, x="label", y="avg_daily_revenue",
        color="label",
        color_discrete_map={
            "Holiday": "#FF6B35",
            "Regular Day": "#4ECDC4",
        },
        text="avg_daily_revenue",
    )
    fig.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
    fig.update_layout(
        paper_bgcolor="#0A0A0F", plot_bgcolor="#13131A",
        font_color="#E8E8F0", showlegend=False,
        xaxis_title="", yaxis_title="Avg Daily Revenue ($)",
    )
    st.plotly_chart(fig, use_container_width=True)

# ── Top days ───────────────────────────────────────────────────────────────
st.subheader("Top 10 Revenue Days")
top_days['revenue'] = top_days['revenue'].astype(float)
st.dataframe(
    top_days.rename(columns={
        'order_date': 'Date',
        'restaurant_id': 'Restaurant',
        'revenue': 'Revenue ($)',
        'orders': 'Orders',
        'is_holiday': 'Holiday',
        'day_of_week': 'Day',
    }),
    hide_index=True,
    use_container_width=True,
)