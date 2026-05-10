import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from utils.athena import run_query

st.set_page_config(page_title="Loyalty Analysis", layout="wide")
st.title("💳 Loyalty Analysis")
st.markdown("Loyalty vs non-loyalty customer behaviour, spend, and trends.")
st.markdown("---")

with st.spinner("Loading loyalty data..."):

    summary = run_query("""
        SELECT loyalty_label,
               CAST(total_customers AS INT) as total_customers,
               CAST(avg_clv AS DOUBLE) as avg_clv,
               CAST(avg_orders AS DOUBLE) as avg_orders,
               CAST(avg_order_value AS DOUBLE) as avg_order_value,
               CAST(avg_days_since_last_order AS DOUBLE) as avg_days_inactive,
               CAST(total_revenue_contribution AS DOUBLE) as total_revenue,
               CAST(median_orders AS DOUBLE) as median_orders
        FROM gp_gold.loyalty_summary
    """)

    by_category = run_query("""
        SELECT item_category, loyalty_label,
               ROUND(CAST(total_revenue AS DOUBLE), 2) as total_revenue,
               CAST(total_orders AS INT) as total_orders,
               CAST(unique_customers AS INT) as unique_customers
        FROM gp_gold.loyalty_by_category
        WHERE item_category IS NOT NULL
        ORDER BY CAST(total_revenue AS DOUBLE) DESC
        LIMIT 30
    """)

    monthly = run_query("""
        SELECT year, month, loyalty_label,
               ROUND(CAST(total_revenue AS DOUBLE), 2) as total_revenue,
               CAST(total_orders AS INT) as total_orders,
               CAST(unique_customers AS INT) as unique_customers
        FROM gp_gold.loyalty_monthly_trend
        ORDER BY year, month, loyalty_label
    """)

# ── KPI row ────────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

loyalty = summary[summary['loyalty_label'] == 'Loyalty Member']
non_loyalty = summary[summary['loyalty_label'] == 'Non-Loyalty']

with col1:
    st.metric("Loyalty Members",
              f"{int(loyalty['total_customers'].values[0]):,}" if len(loyalty) > 0 else "N/A")
with col2:
    st.metric("Non-Loyalty Customers",
              f"{int(non_loyalty['total_customers'].values[0]):,}" if len(non_loyalty) > 0 else "N/A")
with col3:
    st.metric("Loyalty Avg CLV",
              f"${float(loyalty['avg_clv'].values[0]):,.0f}" if len(loyalty) > 0 else "N/A")
with col4:
    st.metric("Non-Loyalty Avg CLV",
              f"${float(non_loyalty['avg_clv'].values[0]):,.0f}" if len(non_loyalty) > 0 else "N/A")

st.markdown("---")

# ── Comparison charts ──────────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)

metrics = ['avg_clv', 'avg_orders', 'avg_order_value']
labels  = ['Avg CLV ($)', 'Avg Orders', 'Avg Order Value ($)']

for i, (metric, label) in enumerate(zip(metrics, labels)):
    with [col1, col2, col3][i]:
        st.subheader(label)
        summary[metric] = summary[metric].astype(float)
        fig = px.bar(
            summary, x="loyalty_label", y=metric,
            color="loyalty_label",
            color_discrete_map={
                "Loyalty Member": "#FF6B35",
                "Non-Loyalty": "#4ECDC4",
            },
            text=metric,
        )
        fmt = '$%{text:,.0f}' if '$' in label else '%{text:,.1f}'
        fig.update_traces(texttemplate=fmt, textposition='outside')
        fig.update_layout(
            paper_bgcolor="#0A0A0F", plot_bgcolor="#13131A",
            font_color="#E8E8F0", showlegend=False,
            xaxis_title="", yaxis_title=label,
            height=300,
        )
        st.plotly_chart(fig, use_container_width=True)

# ── Monthly trend ──────────────────────────────────────────────────────────
st.subheader("Monthly Revenue Trend — Loyalty vs Non-Loyalty")
monthly['period'] = monthly['year'] + '-' + monthly['month'].str.zfill(2)
monthly['total_revenue'] = monthly['total_revenue'].astype(float)
monthly = monthly.sort_values('period')

fig = px.line(
    monthly, x="period", y="total_revenue",
    color="loyalty_label",
    color_discrete_map={
        "Loyalty Member": "#FF6B35",
        "Non-Loyalty": "#4ECDC4",
    },
    markers=True,
)
fig.update_layout(
    paper_bgcolor="#0A0A0F", plot_bgcolor="#13131A",
    font_color="#E8E8F0",
    xaxis_title="Month", yaxis_title="Revenue ($)",
    xaxis=dict(tickangle=45),
    height=400,
)
st.plotly_chart(fig, use_container_width=True)

# ── Top categories ─────────────────────────────────────────────────────────
st.subheader("Revenue by Category — Loyalty vs Non-Loyalty")
by_category['total_revenue'] = by_category['total_revenue'].astype(float)

top_cats = by_category.groupby('item_category')['total_revenue'].sum() \
    .sort_values(ascending=False).head(8).index.tolist()
cat_filtered = by_category[by_category['item_category'].isin(top_cats)]

fig = px.bar(
    cat_filtered, x="item_category", y="total_revenue",
    color="loyalty_label",
    barmode="group",
    color_discrete_map={
        "Loyalty Member": "#FF6B35",
        "Non-Loyalty": "#4ECDC4",
    },
)
fig.update_layout(
    paper_bgcolor="#0A0A0F", plot_bgcolor="#13131A",
    font_color="#E8E8F0",
    xaxis_title="Category", yaxis_title="Revenue ($)",
    xaxis=dict(tickangle=30),
)
st.plotly_chart(fig, use_container_width=True)