import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from utils.athena import run_query

st.set_page_config(page_title="Location Performance", layout="wide")
st.title("📍 Location Performance")
st.markdown("Restaurant rankings by revenue, orders, and customer metrics.")
st.markdown("---")

with st.spinner("Loading location data..."):

    tier_summary = run_query("""
        SELECT performance_tier,
               COUNT(*) as restaurants,
               ROUND(AVG(CAST(total_gross_revenue AS DOUBLE)), 2) as avg_revenue,
               ROUND(AVG(CAST(avg_order_value AS DOUBLE)), 2) as avg_order_value
        FROM gp_gold.location_performance
        GROUP BY performance_tier
        ORDER BY avg_revenue DESC
    """)

    top_restaurants = run_query("""
        SELECT restaurant_id,
               ROUND(CAST(total_gross_revenue AS DOUBLE), 2) as total_revenue,
               CAST(total_orders AS INT) as total_orders,
               CAST(unique_customers AS INT) as unique_customers,
               ROUND(CAST(avg_order_value AS DOUBLE), 2) as avg_order_value,
               ROUND(CAST(loyalty_rate AS DOUBLE), 1) as loyalty_rate_pct,
               performance_tier,
               CAST(revenue_rank AS INT) as rank
        FROM gp_gold.location_performance
        ORDER BY CAST(revenue_rank AS INT)
        LIMIT 20
    """)

    bottom_restaurants = run_query("""
        SELECT restaurant_id,
               ROUND(CAST(total_gross_revenue AS DOUBLE), 2) as total_revenue,
               CAST(total_orders AS INT) as total_orders,
               CAST(unique_customers AS INT) as unique_customers,
               CAST(revenue_rank AS INT) as rank
        FROM gp_gold.location_performance
        ORDER BY CAST(total_gross_revenue AS DOUBLE) ASC
        LIMIT 10
    """)

    monthly_growth = run_query("""
        SELECT restaurant_id, year, month,
               ROUND(CAST(monthly_revenue AS DOUBLE), 2) as monthly_revenue,
               ROUND(CAST(mom_revenue_growth_pct AS DOUBLE), 1) as mom_growth_pct
        FROM gp_gold.location_monthly_performance
        WHERE mom_revenue_growth_pct IS NOT NULL
          AND CAST(revenue_rank AS INT) <= 5
        ORDER BY restaurant_id, year, month
    """ if 'revenue_rank' in [] else """
        SELECT l.restaurant_id,
               l.year, l.month,
               ROUND(CAST(l.monthly_revenue AS DOUBLE), 2) as monthly_revenue,
               ROUND(CAST(l.mom_revenue_growth_pct AS DOUBLE), 1) as mom_growth_pct
        FROM gp_gold.location_monthly_performance l
        JOIN (
            SELECT restaurant_id
            FROM gp_gold.location_performance
            ORDER BY CAST(revenue_rank AS INT)
            LIMIT 5
        ) top ON l.restaurant_id = top.restaurant_id
        WHERE l.mom_revenue_growth_pct IS NOT NULL
        ORDER BY l.restaurant_id, l.year, l.month
    """)

# ── KPI row ────────────────────────────────────────────────────────────────
tier_summary['restaurants'] = tier_summary['restaurants'].astype(int)
tier_summary['avg_revenue'] = tier_summary['avg_revenue'].astype(float)

col1, col2, col3 = st.columns(3)
top10 = tier_summary[tier_summary['performance_tier'] == 'TOP_10']
standard = tier_summary[tier_summary['performance_tier'] == 'STANDARD']

with col1:
    st.metric("Total Restaurants", f"{tier_summary['restaurants'].sum():,}")
with col2:
    st.metric("TOP_10 Avg Revenue",
              f"${float(top10['avg_revenue'].values[0]):,.0f}" if len(top10) > 0 else "N/A")
with col3:
    st.metric("STANDARD Avg Revenue",
              f"${float(standard['avg_revenue'].values[0]):,.0f}" if len(standard) > 0 else "N/A")

st.markdown("---")

# ── Performance tier ───────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Performance Tier Distribution")
    fig = px.pie(
        tier_summary,
        values="restaurants", names="performance_tier",
        color_discrete_sequence=["#FF6B35", "#FFB347", "#4ECDC4", "#888"],
        hole=0.4,
    )
    fig.update_layout(
        paper_bgcolor="#0A0A0F", font_color="#E8E8F0",
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Avg Revenue by Tier")
    fig = px.bar(
        tier_summary,
        x="performance_tier", y="avg_revenue",
        color="performance_tier",
        color_discrete_sequence=["#FF6B35", "#FFB347", "#4ECDC4", "#888"],
        text="avg_revenue",
    )
    fig.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
    fig.update_layout(
        paper_bgcolor="#0A0A0F", plot_bgcolor="#13131A",
        font_color="#E8E8F0", showlegend=False,
        xaxis_title="Tier", yaxis_title="Avg Revenue ($)",
    )
    st.plotly_chart(fig, use_container_width=True)

# ── Top 20 restaurants ─────────────────────────────────────────────────────
st.subheader("Top 20 Restaurants by Revenue")
top_restaurants['total_revenue'] = top_restaurants['total_revenue'].astype(float)

fig = px.bar(
    top_restaurants.head(10),
    x="total_revenue", y="restaurant_id",
    orientation="h",
    color="performance_tier",
    color_discrete_map={
        "TOP_10": "#FF6B35", "TOP_25": "#FFB347",
        "TOP_50": "#4ECDC4", "STANDARD": "#888",
    },
    text="total_revenue",
)
fig.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
fig.update_layout(
    paper_bgcolor="#0A0A0F", plot_bgcolor="#13131A",
    font_color="#E8E8F0",
    yaxis_title="", xaxis_title="Total Revenue ($)",
    height=400,
)
st.plotly_chart(fig, use_container_width=True)

st.dataframe(
    top_restaurants.rename(columns={
        'restaurant_id': 'Restaurant',
        'total_revenue': 'Revenue ($)',
        'total_orders': 'Orders',
        'unique_customers': 'Customers',
        'avg_order_value': 'AOV ($)',
        'loyalty_rate_pct': 'Loyalty Rate (%)',
        'performance_tier': 'Tier',
        'rank': 'Rank',
    }),
    hide_index=True,
    use_container_width=True,
)