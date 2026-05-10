import streamlit as st

st.set_page_config(
    page_title  = "GlobalPartners Analytics",
    page_icon   = "🍽️",
    layout      = "wide",
    initial_sidebar_state = "expanded",
)

# ── Custom CSS ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* Dark industrial theme */
    @import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Syne:wght@400;600;700;800&display=swap');

    html, body, [class*="css"] {
        font-family: 'Syne', sans-serif;
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: #0D0D14;
        border-right: 1px solid #1E1E2E;
    }

    /* Metric cards */
    [data-testid="metric-container"] {
        background: #13131A;
        border: 1px solid #1E1E2E;
        border-radius: 8px;
        padding: 16px;
    }

    /* Headers */
    h1, h2, h3 {
        font-family: 'Syne', sans-serif;
        font-weight: 800;
        letter-spacing: -0.5px;
    }

    /* Dataframes */
    [data-testid="stDataFrame"] {
        border: 1px solid #1E1E2E;
        border-radius: 8px;
    }

    /* Orange accent on metrics */
    [data-testid="metric-container"] [data-testid="stMetricValue"] {
        color: #FF6B35;
        font-family: 'DM Mono', monospace;
        font-size: 2rem;
    }

    /* Tab styling */
    .stTabs [data-baseweb="tab"] {
        font-family: 'DM Mono', monospace;
        font-size: 0.85rem;
        letter-spacing: 1px;
        text-transform: uppercase;
    }

    /* Divider */
    hr {
        border-color: #1E1E2E;
    }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🍽️ GlobalPartners")
    st.markdown("**Restaurant Analytics Platform**")
    st.markdown("---")
    st.markdown("### Navigation")
    st.markdown("""
    - 📊 **Overview** — KPI summary
    - 👥 **Customers** — CLV & RFM
    - ⚠️ **Churn** — At-risk customers
    - 📈 **Sales Trends** — Revenue over time
    - 📍 **Locations** — Restaurant performance
    - 💳 **Loyalty** — Member analysis
    - 🏷️ **Discounts** — Promotion impact
    """)
    st.markdown("---")
    st.caption("Data refreshed daily at 2AM UTC")
    st.caption("Source: AWS Glue → S3 → Athena")

# ── Home page ────────────────────────────────────────────────────────────────
st.title("GlobalPartners Analytics")
st.markdown("#### Restaurant Group — Business Intelligence Dashboard")
st.markdown("---")

st.markdown("""
Select a page from the sidebar to explore analytics across customers,
locations, sales trends, loyalty, and discount effectiveness.
""")

col1, col2, col3 = st.columns(3)

with col1:
    st.info("👥 **Customer Analytics**\nCLV bands, RFM segments, top customers by lifetime value")

with col2:
    st.warning("⚠️ **Churn & Retention**\nAt-risk customers, spend trends, inactivity alerts")

with col3:
    st.success("📈 **Sales Trends**\nDaily, weekly, monthly revenue with holiday enrichment")