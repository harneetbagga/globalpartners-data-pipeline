import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from utils.athena import run_query

st.set_page_config(page_title="Discount Effectiveness", layout="wide")
st.title("🏷️ Discount Effectiveness")
st.markdown("Impact of discounts on order behaviour, categories, and customer segments.")
st.markdown("---")

with st.spinner("Loading discount data..."):

    order_summary = run_query("""
        SELECT discount_label,
               CAST(total_orders AS INT) as total_orders,
               CAST(avg_gross_order_value AS DOUBLE) as avg_gross_order_value,
               CAST(avg_net_order_value AS DOUBLE) as avg_net_order_value,
               CAST(avg_items_per_order AS DOUBLE) as avg_items_per_order,
               CAST(avg_discount_amount AS DOUBLE) as avg_discount_amount,
               CAST(unique_customers AS INT) as unique_customers
        FROM gp_gold.discount_order_summary
    """)

    by_category = run_query("""
        SELECT item_category, discount_label,
            ROUND(CAST(total_gross_revenue AS DOUBLE), 2) as total_revenue,
            ROUND(CAST(total_discount_given AS DOUBLE), 2) as total_discount,
            ROUND(CAST(avg_discount_depth_pct AS DOUBLE), 1) as avg_discount_pct,
            CAST(total_orders AS INT) as total_orders
        FROM gp_gold.discount_by_category
        WHERE item_category IS NOT NULL
        ORDER BY CAST(total_gross_revenue AS DOUBLE) DESC
        LIMIT 30
    """)

    segments = run_query("""
        SELECT discount_seeker_segment,
               COUNT(*) as customers,
               ROUND(AVG(CAST(total_net_spend AS DOUBLE)), 2) as avg_spend,
               ROUND(AVG(CAST(discount_usage_rate AS DOUBLE)), 1) as avg_discount_rate,
               ROUND(AVG(CAST(total_discounts_received AS DOUBLE)), 2) as avg_discounts_received
        FROM gp_gold.discount_customer_behaviour
        GROUP BY discount_seeker_segment
        ORDER BY customers DESC
    """)

# ── KPI row ────────────────────────────────────────────────────────────────
discounted = order_summary[order_summary['discount_label'] == 'Discounted']
non_disc   = order_summary[order_summary['discount_label'] == 'Non-Discounted']

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Discounted Orders",
              f"{int(discounted['total_orders'].values[0]):,}" if len(discounted) > 0 else "N/A")
with col2:
    st.metric("Non-Discounted Orders",
              f"{int(non_disc['total_orders'].values[0]):,}" if len(non_disc) > 0 else "N/A")
with col3:
    st.metric("Avg Discount Amount",
              f"${float(discounted['avg_discount_amount'].values[0]):,.2f}" if len(discounted) > 0 else "N/A")
with col4:
    st.metric("Avg Items (Discounted)",
              f"{float(discounted['avg_items_per_order'].values[0]):,.1f}" if len(discounted) > 0 else "N/A")

st.markdown("---")

# ── Order value comparison ─────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Avg Order Value — Discounted vs Non-Discounted")
    order_summary['avg_gross_order_value'] = order_summary['avg_gross_order_value'].astype(float)
    fig = px.bar(
        order_summary, x="discount_label", y="avg_gross_order_value",
        color="discount_label",
        color_discrete_map={
            "Discounted": "#FF6B35",
            "Non-Discounted": "#4ECDC4",
        },
        text="avg_gross_order_value",
    )
    fig.update_traces(texttemplate='$%{text:,.2f}', textposition='outside')
    fig.update_layout(
        paper_bgcolor="#0A0A0F", plot_bgcolor="#13131A",
        font_color="#E8E8F0", showlegend=False,
        xaxis_title="", yaxis_title="Avg Gross Order Value ($)",
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Avg Items per Order")
    order_summary['avg_items_per_order'] = order_summary['avg_items_per_order'].astype(float)
    fig = px.bar(
        order_summary, x="discount_label", y="avg_items_per_order",
        color="discount_label",
        color_discrete_map={
            "Discounted": "#FF6B35",
            "Non-Discounted": "#4ECDC4",
        },
        text="avg_items_per_order",
    )
    fig.update_traces(texttemplate='%{text:,.2f}', textposition='outside')
    fig.update_layout(
        paper_bgcolor="#0A0A0F", plot_bgcolor="#13131A",
        font_color="#E8E8F0", showlegend=False,
        xaxis_title="", yaxis_title="Avg Items per Order",
    )
    st.plotly_chart(fig, use_container_width=True)

# ── Discount seeker segments ───────────────────────────────────────────────
st.subheader("Customer Discount Seeker Segments")
segments['customers'] = segments['customers'].astype(int)
segments['avg_spend'] = segments['avg_spend'].astype(float)

col1, col2 = st.columns([2, 1])

with col1:
    seg_colors = {
        "NON_DISCOUNT_USER": "#4ECDC4",
        "OCCASIONAL_DISCOUNT_USER": "#FFB347",
        "MODERATE_DISCOUNT_USER": "#FF6B35",
        "HEAVY_DISCOUNT_USER": "#FF4444",
    }
    fig = px.bar(
        segments, x="discount_seeker_segment", y="customers",
        color="discount_seeker_segment",
        color_discrete_map=seg_colors,
        text="customers",
    )
    fig.update_traces(textposition='outside')
    fig.update_layout(
        paper_bgcolor="#0A0A0F", plot_bgcolor="#13131A",
        font_color="#E8E8F0", showlegend=False,
        xaxis_title="Segment", yaxis_title="Customers",
        xaxis=dict(tickangle=20),
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.dataframe(
        segments.rename(columns={
            'discount_seeker_segment': 'Segment',
            'customers': 'Customers',
            'avg_spend': 'Avg Spend ($)',
            'avg_discount_rate': 'Discount Rate (%)',
            'avg_discounts_received': 'Avg Discount ($)',
        }),
        hide_index=True,
        use_container_width=True,
    )

# ── Top categories by discount ─────────────────────────────────────────────
st.subheader("Top Categories by Discount Amount Given")
disc_cats = by_category[by_category['discount_label'] == 'Discounted'] \
    .sort_values('total_discount', ascending=False).head(10)
disc_cats['total_discount'] = disc_cats['total_discount'].astype(float)

fig = px.bar(
    disc_cats, x="total_discount", y="item_category",
    orientation="h",
    color="avg_discount_pct",
    color_continuous_scale="Reds",
    text="total_discount",
)
fig.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
fig.update_layout(
    paper_bgcolor="#0A0A0F", plot_bgcolor="#13131A",
    font_color="#E8E8F0",
    yaxis_title="", xaxis_title="Total Discount Given ($)",
    coloraxis_colorbar_title="Avg %",
)
st.plotly_chart(fig, use_container_width=True)