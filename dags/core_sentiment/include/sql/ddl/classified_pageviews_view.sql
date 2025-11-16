-- ======================================================================================
-- ========================= CLASSIFIED PAGEVIEWS VIEW ==================================
-- ==== Purpose: Materialized view of filtered pageviews with company classification ====
-- ========== Business Value: Pre-computed classification for fast analytics ============
-- ======================================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS v_classified_pageviews AS
SELECT 
    f.id,
    f.domain,
    f.page_title,
    f.count_views,
    f.processing_date,
    f.filtered_at,
    f.filter_method,
    get_company_with_overrides(f.page_title) as company,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM company_overrides o 
            WHERE o.page_title = f.page_title
        ) THEN true 
        ELSE false 
    END as is_manual_override
FROM filtered_pageviews f
WHERE get_company_with_overrides(f.page_title) != 'Other'  -- Exclude unclassified
ORDER BY f.processing_date DESC, f.count_views DESC;

-- Create indexes on materialized view
CREATE INDEX IF NOT EXISTS idx_classified_company 
    ON v_classified_pageviews(company);

CREATE INDEX IF NOT EXISTS idx_classified_date 
    ON v_classified_pageviews(processing_date DESC);

CREATE INDEX IF NOT EXISTS idx_classified_views 
    ON v_classified_pageviews(count_views DESC);

CREATE INDEX IF NOT EXISTS idx_classified_company_date 
    ON v_classified_pageviews(company, processing_date);

COMMENT ON MATERIALIZED VIEW v_classified_pageviews IS 
    'Pre-computed company classifications for all filtered pageviews.
    Refresh after loading new filtered data.
    This is the primary source for analytics queries.';


-- ======================================================================================
-- ================================== REFRESH FUNCTION ==================================
-- =============== Purpose: Convenient way to refresh the materialized view =============
-- ======================================================================================

CREATE OR REPLACE FUNCTION refresh_classified_pageviews()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY v_classified_pageviews;
    RAISE NOTICE 'Materialized view v_classified_pageviews refreshed';
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION refresh_classified_pageviews IS 
    'Refreshes the classified pageviews materialized view.
    Call this after loading new filtered data or updating overrides.
    Uses CONCURRENTLY to avoid locking.';


-- ======================================================================================
-- ======================= CONVENIENCE VIEWS FOR COMMON QUERIES =========================
-- ======================================================================================

-- Today's classified pageviews
CREATE OR REPLACE VIEW v_today_classified AS
SELECT *
FROM v_classified_pageviews
WHERE processing_date = CURRENT_DATE;

COMMENT ON VIEW v_today_classified IS 
    'Shortcut view for today''s classified pageviews';


-- Latest 7 days
CREATE OR REPLACE VIEW v_week_classified AS
SELECT *
FROM v_classified_pageviews
WHERE processing_date >= CURRENT_DATE - INTERVAL '7 days';

COMMENT ON VIEW v_week_classified IS 
    'Shortcut view for last 7 days of classified pageviews';


-- =====================================================================================
-- ================================== STATISTICS VIEW ==================================
-- ===================== Shows classification distribution =============================
-- =====================================================================================

CREATE OR REPLACE VIEW v_classification_stats AS
SELECT 
    processing_date,
    company,
    COUNT(*) as page_count,
    SUM(count_views) as total_views,
    AVG(count_views) as avg_views,
    MAX(count_views) as max_views,
    COUNT(*) FILTER (WHERE is_manual_override) as override_count
FROM v_classified_pageviews
GROUP BY processing_date, company
ORDER BY processing_date DESC, total_views DESC;

COMMENT ON VIEW v_classification_stats IS 
    'Daily statistics by company from classified pageviews.
    Shows distribution and override usage.';