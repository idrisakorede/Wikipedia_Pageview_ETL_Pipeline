WITH company_totals AS (
    SELECT 
        company,
        COUNT(*) as page_count,
        SUM(count_views) as total_views
    FROM v_today_classified
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
    ROUND(100.0 * (w.total_views - r.runner_up_views) / r.runner_up_views, 2) as lead_percentage,
    (SELECT SUM(total_views) FROM company_totals) as grand_total_views,
    ROUND(100.0 * w.total_views / (SELECT SUM(total_views) FROM company_totals), 2) as winner_market_share,
    CURRENT_DATE as report_date
FROM winner w
CROSS JOIN runner_up r
WHERE w.rank = 1;

-- ==========================================
-- EXAMPLE OUTPUT
-- ==========================================
-- winner_company    | Apple
-- winner_page_count | 145
-- winner_total_views| 2,450,000
-- runner_up_company | Google
-- runner_up_views   | 2,100,000
-- lead_by_views     | 350,000
-- lead_percentage   | 16.67
-- grand_total_views | 8,500,000
-- winner_market_share| 28.82
-- report_date       | 2025-11-15