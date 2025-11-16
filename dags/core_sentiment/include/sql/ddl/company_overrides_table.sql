-- ==============================================================================
-- ======================= COMPANY OVERRIDES TABLE ==============================
-- =========== Purpose: Manual corrections for misclassified pages ==============
-- ==== Business Use: Handle edge cases where automated classification fails ====
-- ==============================================================================

CREATE TABLE IF NOT EXISTS company_overrides (
    id SERIAL PRIMARY KEY,
    page_title TEXT NOT NULL UNIQUE,
    correct_company VARCHAR(50) NOT NULL,
    reason TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    
    CHECK (correct_company IN ('Amazon', 'Apple', 'Google', 'Microsoft', 'Meta', 'Other'))
);

CREATE INDEX IF NOT EXISTS idx_overrides_page_title 
    ON company_overrides(page_title);

COMMENT ON TABLE company_overrides IS 
    'Manual overrides for company classification.
    Takes precedence over automated classify_company() function.
    Use for edge cases and misclassifications.';

COMMENT ON COLUMN company_overrides.reason IS 
    'Explanation for why this page needs manual override';

COMMENT ON COLUMN company_overrides.created_by IS 
    'Username or system that created this override';


-- ===========================================================================
-- ================ HELPER FUNCTION: Get company with overrides ==============
-- ==== Purpose: Check overrides first, then fall back to classification =====
-- ===========================================================================

CREATE OR REPLACE FUNCTION get_company_with_overrides(page_title TEXT)
RETURNS VARCHAR(50) AS $$
DECLARE
    override_company VARCHAR(50);
BEGIN
    -- Check for manual override first
    SELECT correct_company INTO override_company
    FROM company_overrides
    WHERE company_overrides.page_title = get_company_with_overrides.page_title;
    
    -- Return override if found
    IF FOUND THEN
        RETURN override_company;
    END IF;
    
    -- Otherwise use automated classification
    RETURN classify_company(page_title);
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION get_company_with_overrides IS 
    'Gets company classification with manual overrides applied.
    Use this function in queries instead of classify_company() directly.';


-- ===========================================================================
-- ======================= VIEW: Pages needing review ========================
-- ============ Shows pages classified as 'Other' with high views ============
-- ===========================================================================

CREATE OR REPLACE VIEW v_pages_needing_review AS
SELECT 
    page_title,
    count_views,
    processing_date
FROM filtered_pageviews
WHERE classify_company(page_title) = 'Other'
    AND count_views >= 1000  -- High traffic pages worth reviewing
    AND processing_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY count_views DESC
LIMIT 100;

COMMENT ON VIEW v_pages_needing_review IS 
    'High-traffic pages classified as ''Other'' that may need manual override.
    Review these periodically to improve classification accuracy.';