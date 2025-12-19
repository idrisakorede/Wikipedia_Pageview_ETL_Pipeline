"""
Core Sentiment Dashboard - Streamlit App
Shows pageview analytics and company rankings based on filtered_pageviews table.
"""

import logging
import os
from datetime import datetime

import pandas as pd
import plotly.express as px
import psycopg
import streamlit as st

logger = logging.getLogger(__name__)

# Page config
st.set_page_config(
    page_title="Core Sentiment Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .main {padding-top: 0rem;}
    .stMetric {background-color: #f0f2f6; padding: 15px; border-radius: 5px;}
    </style>
""",
    unsafe_allow_html=True,
)

# ========================================================
# DATABASE CONNECTION
# ========================================================


@st.cache_resource
def get_connection():
    """Create database connection using psycopg."""
    db_name = os.getenv("DB_NAME", "core_sentiment")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "postgres")
    db_host = "core_sentiment_db"
    db_port = 5432

    try:
        conn = psycopg.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password,
            connect_timeout=10,
        )
        return conn
    except Exception as e:
        st.error(f"❌ Database connection failed: {e}")
        st.info(f"Host: {db_host}, DB: {db_name}, User: {db_user}")
        st.stop()


# ========================================================
# DATA LOADERS
# ========================================================


@st.cache_data(ttl=300)
def load_filtered_pageviews(days=7, companies=None):
    """Load filtered pageviews with company classification."""
    conn = get_connection()

    company_filter = ""
    if companies and len(companies) > 0:
        placeholders = ",".join(["%s"] * len(companies))
        company_filter = f"AND company IN ({placeholders})"

    query = f"""
        SELECT 
            domain,
            page_title,
            count_views,
            company,
            processing_date,
            filtered_at,
            filter_method
        FROM filtered_pageviews
        WHERE processing_date >= CURRENT_DATE - INTERVAL '{days} days'
        {company_filter}
        ORDER BY processing_date DESC, count_views DESC
    """

    if companies:
        return pd.read_sql(query, conn, params=companies)  # type: ignore
    return pd.read_sql(query, conn)  # type: ignore


@st.cache_data(ttl=300)
def get_company_rankings(days=7, companies=None):
    """Get company rankings."""
    conn = get_connection()

    query = f"""
        WITH daily_metrics AS (
            SELECT 
                company,
                COUNT(*) as page_count,
                SUM(count_views) as total_views,
                ROUND(AVG(count_views), 2) as avg_views,
                MAX(count_views) as max_views,
                MIN(count_views) as min_views,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY count_views) as median_views
            FROM filtered_pageviews
            WHERE processing_date >= CURRENT_DATE - INTERVAL '{days} days'
            GROUP BY company
        ),
        rankings AS (
            SELECT 
                *,
                RANK() OVER (ORDER BY total_views DESC) as rank,
                ROUND(100.0 * total_views / SUM(total_views) OVER (), 2) as market_share_pct
            FROM daily_metrics
        )
        SELECT 
            rank,
            company,
            page_count,
            total_views,
            avg_views,
            median_views,
            max_views,
            min_views,
            market_share_pct
        FROM rankings
        ORDER BY rank
    """

    df = pd.read_sql(query, conn)  # type: ignore

    # Filter by companies if specified
    if companies:
        df = df[df["company"].isin(companies)]

    return df


@st.cache_data(ttl=300)
def get_daily_trends(days=7, companies=None):
    """Get daily view trends by company."""
    conn = get_connection()

    company_filter = ""
    if companies and len(companies) > 0:
        placeholders = ",".join(["%s"] * len(companies))
        company_filter = f"AND company IN ({placeholders})"

    query = f"""
        SELECT 
            processing_date,
            company,
            COUNT(*) as page_count,
            SUM(count_views) as total_views
        FROM filtered_pageviews
        WHERE processing_date >= CURRENT_DATE - INTERVAL '{days} days'
        {company_filter}
        GROUP BY processing_date, company
        ORDER BY processing_date, company
    """

    if companies:
        return pd.read_sql(query, conn, params=companies)  # type: ignore
    return pd.read_sql(query, conn)  # type: ignore


@st.cache_data(ttl=300)
def get_data_quality():
    """Get data quality metrics."""
    conn = get_connection()
    query = """
        SELECT * FROM v_data_quality_metrics
        ORDER BY processing_date DESC
        LIMIT 30
    """
    try:
        return pd.read_sql(query, conn)  # type: ignore
    except Exception as e:
        logger.warning(f"Could not load data quality metrics: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def get_winner_analysis(days=7):
    """Get winner vs runner-up analysis."""
    conn = get_connection()

    query = f"""
        WITH company_totals AS (
            SELECT 
                company,
                COUNT(*) as page_count,
                SUM(count_views) as total_views
            FROM filtered_pageviews
            WHERE processing_date >= CURRENT_DATE - INTERVAL '{days} days'
            GROUP BY company
        ),
        winner AS (
            SELECT 
                company,
                page_count,
                total_views,
                RANK() OVER (ORDER BY total_views DESC) as rank
            FROM company_totals
        ),
        runner_up AS (
            SELECT 
                company as runner_up_company,
                total_views as runner_up_views
            FROM winner
            WHERE rank = 2
        )
        SELECT 
            w.company as winner_company,
            w.page_count as winner_page_count,
            w.total_views as winner_total_views,
            r.runner_up_company,
            r.runner_up_views,
            w.total_views - r.runner_up_views as lead_by_views,
            ROUND(100.0 * (w.total_views - r.runner_up_views) / NULLIF(r.runner_up_views, 0), 2) as lead_percentage,
            (SELECT SUM(total_views) FROM company_totals) as grand_total_views,
            ROUND(100.0 * w.total_views / NULLIF((SELECT SUM(total_views) FROM company_totals), 0), 2) as winner_market_share
        FROM winner w
        CROSS JOIN runner_up r
        WHERE w.rank = 1
    """
    try:
        df = pd.read_sql(query, conn)  # type: ignore
        if not df.empty:
            return df.iloc[0]
        return None
    except (psycopg.Error, IndexError) as e:
        logger.error(f"Error fetching winner analysis: {e}")
        return None


# ========================================================
# HEADER & SIDEBAR
# ========================================================

st.title("📊 Core Sentiment Analytics Dashboard")
st.markdown("*Real-time Wikipedia pageview analytics for tech companies*")

st.sidebar.header("⚙️ Filters")
days = st.sidebar.slider("📅 Time Period (Days)", 1, 30, 7)

all_companies = ["Amazon", "Apple", "Google", "Microsoft", "Meta"]
companies = st.sidebar.multiselect(
    "🏢 Select Companies", all_companies, default=all_companies
)

if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.caption(f"📡 Connected to: `{os.getenv('DB_NAME', 'core_sentiment')}`")

# ========================================================
# LOAD DATA
# ========================================================

with st.spinner("⏳ Loading data..."):
    try:
        df = load_filtered_pageviews(days, companies)
        rankings_df = get_company_rankings(days, companies)
        trends_df = get_daily_trends(days, companies)
        quality_df = get_data_quality()
        winner_data = get_winner_analysis(days)
    except Exception as e:
        st.error(f"❌ Error loading data: {e}")
        st.stop()

if df.empty:
    st.warning(f"⚠️ No data found for the last {days} days")
    st.info("💡 Try running your Airflow pipeline to generate data")
    st.stop()

# ========================================================
# WINNER SPOTLIGHT
# ========================================================

if winner_data is not None:
    st.subheader("🏆 Market Leader Analysis")

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "👑 Winner",
        winner_data["winner_company"],
        f"{winner_data['winner_total_views']:,.0f} views",
    )
    col2.metric(
        "📊 Market Share",
        f"{winner_data['winner_market_share']:.1f}%",
        f"{winner_data['winner_page_count']} pages",
    )
    col3.metric(
        "🥈 Runner-Up",
        winner_data["runner_up_company"],
        f"{winner_data['runner_up_views']:,.0f} views",
    )
    col4.metric(
        "📈 Lead By",
        f"{winner_data['lead_percentage']:.1f}%",
        f"{winner_data['lead_by_views']:,.0f} views",
    )

st.markdown("---")

# ========================================================
# KEY METRICS
# ========================================================

st.subheader("📈 Overview Metrics")
col1, col2, col3, col4 = st.columns(4)

col1.metric("📄 Total Pages", f"{len(df):,}")
col2.metric("👁️ Total Views", f"{df['count_views'].sum():,}")
col3.metric("📊 Avg Views/Page", f"{df['count_views'].mean():,.0f}")
col4.metric("🏢 Companies", df["company"].nunique())

st.markdown("---")

# ========================================================
# COMPANY RANKINGS
# ========================================================

st.subheader("🏅 Company Rankings")

if not rankings_df.empty:
    col1, col2 = st.columns([2, 1])

    with col1:
        display_df = rankings_df.copy()
        display_df["rank"] = display_df["rank"].astype(int)

        st.dataframe(
            display_df.style.format(
                {
                    "total_views": "{:,.0f}",
                    "avg_views": "{:,.2f}",
                    "median_views": "{:,.0f}",
                    "max_views": "{:,.0f}",
                    "min_views": "{:,.0f}",
                    "market_share_pct": "{:.2f}%",
                    "page_count": "{:,.0f}",
                }
            ),
            hide_index=True,
            use_container_width=True,
            height=250,
        )

    with col2:
        fig_pie = px.pie(
            rankings_df,
            values="total_views",
            names="company",
            title="Market Share by Views",
            color="company",
            color_discrete_map={
                "Amazon": "#FF9900",
                "Apple": "#A2AAAD",
                "Google": "#4285F4",
                "Microsoft": "#00A4EF",
                "Meta": "#0668E1",
            },
        )
        fig_pie.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig_pie, use_container_width=True)

st.markdown("---")

# ========================================================
# TRENDS OVER TIME
# ========================================================

st.subheader("📅 Daily Trends")

if not trends_df.empty:
    fig_trend = px.line(
        trends_df,
        x="processing_date",
        y="total_views",
        color="company",
        title=f"Daily Total Views by Company (Last {days} Days)",
        markers=True,
        color_discrete_map={
            "Amazon": "#FF9900",
            "Apple": "#A2AAAD",
            "Google": "#4285F4",
            "Microsoft": "#00A4EF",
            "Meta": "#0668E1",
        },
    )
    fig_trend.update_layout(
        xaxis_title="Date", yaxis_title="Total Views", hovermode="x unified", height=400
    )
    st.plotly_chart(fig_trend, use_container_width=True)

st.markdown("---")

# ========================================================
# TOP PAGES BY COMPANY
# ========================================================

st.subheader("🔝 Top Pages by Company")

tabs = st.tabs(["🟠 Amazon", "⚪ Apple", "🔵 Google", "🔷 Microsoft", "🟦 Meta"])

for tab, company in zip(tabs, ["Amazon", "Apple", "Google", "Microsoft", "Meta"]):
    with tab:
        company_data = df[df["company"] == company].nlargest(15, "count_views")

        if not company_data.empty:
            st.dataframe(
                company_data[
                    ["page_title", "count_views", "processing_date"]
                ].style.format({"count_views": "{:,.0f}"}),
                hide_index=True,
                use_container_width=True,
                height=400,
            )
        else:
            st.info(f"ℹ️ No data available for {company} in the selected period")

st.markdown("---")

# ========================================================
# DATA QUALITY
# ========================================================

if not quality_df.empty:
    st.subheader("🔍 Data Quality Metrics")

    col1, col2 = st.columns(2)

    with col1:
        st.dataframe(
            quality_df[
                [
                    "processing_date",
                    "raw_records",
                    "filtered_records",
                    "filter_rate_pct",
                ]
            ]
            .head(10)
            .style.format(
                {
                    "raw_records": "{:,.0f}",
                    "filtered_records": "{:,.0f}",
                    "filter_rate_pct": "{:.2f}%",
                }
            ),
            hide_index=True,
            use_container_width=True,
        )

    with col2:
        fig_quality = px.bar(
            quality_df.head(10),
            x="processing_date",
            y=["raw_records", "filtered_records"],
            title="Raw vs Filtered Records",
            barmode="group",
            color_discrete_sequence=["#636EFA", "#00CC96"],
        )
        st.plotly_chart(fig_quality, use_container_width=True)

    st.markdown("---")

# ========================================================
# DATA EXPLORER
# ========================================================

with st.expander("🔎 Detailed Data Explorer"):
    st.subheader("Search & Filter")

    search = st.text_input("🔍 Search page titles", "")
    filtered_table = (
        df[df["page_title"].str.contains(search, case=False, na=False)]
        if search
        else df
    )

    st.dataframe(
        filtered_table[
            ["processing_date", "page_title", "company", "count_views", "filter_method"]
        ].sort_values("count_views", ascending=False),
        hide_index=True,
        use_container_width=True,
        height=400,
    )

    csv = filtered_table.to_csv(index=False)
    st.download_button(
        label="📥 Download as CSV",
        data=csv,
        file_name=f"core_sentiment_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
    )

# ========================================================
# FOOTER
# ========================================================

st.markdown("---")
col1, col2, col3 = st.columns(3)

col1.caption(f"📅 Data Period: Last {days} days")
col2.caption(f"🔄 Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
if not df.empty:
    col3.caption(f"🎯 Filter Method: {df['filter_method'].iloc[0]}")
