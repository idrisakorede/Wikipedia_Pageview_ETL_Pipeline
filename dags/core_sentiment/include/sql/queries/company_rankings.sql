-- ==========================================
-- COMPANY RANKINGS QUERY
-- Business Question: How do tech companies rank by Wikipedia pageviews today?
-- Output: Daily rankings with comprehensive metrics
-- ==========================================

WITH daily_metrics AS (
    SELECT 
        company,
        COUNT(*) as page_count,
        SUM(count_views) as total_views,
        ROUND(AVG(count_views), 2) as avg_views,
        MAX(count_views) as max_views,
        MIN(count_views) as min_views,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY count_views) as median_views
    FROM v_today_classified
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
    market_share_pct,
    CURRENT_DATE as report_date
FROM rankings
ORDER BY rank;

-- ==========================================
-- USAGE IN AIRFLOW
-- ==========================================
-- SQLExecuteQueryOperator(
--     task_id="get_company_rankings",
--     sql="queries/company_rankings.sql",
--     conn_id="core_sentiment_db",
--     do_xcom_push=True
-- )