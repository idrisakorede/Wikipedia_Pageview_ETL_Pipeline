-- ===================================================================
-- ================== RAW DATA WAREHOUSE TABLES ======================
-- Purpose: Immutable storage of all extracted Wikipedia pageview data
-- ===================================================================

-- Enable extensions (if needed)
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- For fuzzy text search

-- Raw pageviews: Complete historical record
CREATE TABLE IF NOT EXISTS raw_pageviews (
    id BIGSERIAL PRIMARY KEY,
    domain VARCHAR(255) NOT NULL,
    page_title TEXT NOT NULL,
    count_views INTEGER NOT NULL CHECK (count_views >= 0),
    source_file VARCHAR(500) NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processing_date DATE NOT NULL
);

-- Indexes for warehouse queries
CREATE INDEX IF NOT EXISTS idx_raw_processing_date 
    ON raw_pageviews(processing_date DESC);

CREATE INDEX IF NOT EXISTS idx_raw_domain 
    ON raw_pageviews(domain);

CREATE INDEX IF NOT EXISTS idx_raw_views 
    ON raw_pageviews(count_views DESC);

CREATE INDEX IF NOT EXISTS idx_raw_page_title_pattern 
    ON raw_pageviews USING gin(page_title gin_trgm_ops);  -- Trigram index for LIKE queries

-- Partitioning hint comment (implement if data volume grows)
COMMENT ON TABLE raw_pageviews IS 
    'Warehouse table for all Wikipedia pageviews. 
    Consider partitioning by processing_date when data exceeds 10M rows.
    Retention policy: 90 days recommended.';



-- ===================================================================
-- ============== FILTERED PAGEVIEWS (Curated Layer) =================
-- ========= Purpose: LLM-filtered product/service pages only ========
-- ===================================================================

CREATE TABLE IF NOT EXISTS filtered_pageviews (
    id BIGSERIAL PRIMARY KEY,
    domain VARCHAR(255) NOT NULL,
    page_title TEXT NOT NULL,
    count_views INTEGER NOT NULL CHECK (count_views >= 0),
    filtered_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processing_date DATE NOT NULL,
    filter_method VARCHAR(100) NOT NULL,
    filter_confidence NUMERIC(3,2), -- Optional: 0.00 to 1.00
    
    CONSTRAINT unique_filtered_pageview UNIQUE (domain, page_title, processing_date, filter_method)
);

CREATE INDEX IF NOT EXISTS idx_filtered_processing_date 
    ON filtered_pageviews(processing_date DESC);

CREATE INDEX IF NOT EXISTS idx_filtered_page_title 
    ON filtered_pageviews(page_title);

CREATE INDEX IF NOT EXISTS idx_filtered_views 
    ON filtered_pageviews(count_views DESC);

COMMENT ON TABLE filtered_pageviews IS 
    'Curated dataset of LLM-filtered product/service pages.
    Used as source for company classification and analytics.';

COMMENT ON COLUMN filtered_pageviews.filter_method IS 
    'Identifier for filtering method (e.g., llm_ollama_llama3.2:1b)';


-- ===================================================================
-- ========== COMPANY PAGEVIEW SUMMARY (Analytics Layer) =============
-- =========== Purpose: Daily aggregated metrics by company ==========
-- ===================================================================

CREATE TABLE IF NOT EXISTS company_pageview_summary (
    id SERIAL PRIMARY KEY,
    company VARCHAR(50) NOT NULL,
    page_count INTEGER NOT NULL CHECK (page_count > 0),
    total_views BIGINT NOT NULL CHECK (total_views > 0),
    avg_views NUMERIC(12,2) NOT NULL,
    max_views INTEGER NOT NULL,
    min_views INTEGER NOT NULL,
    processing_date DATE NOT NULL,
    calculated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Business rule: One summary per company per day
    CONSTRAINT unique_company_date UNIQUE (company, processing_date)
);

CREATE INDEX IF NOT EXISTS idx_summary_date 
    ON company_pageview_summary(processing_date DESC);

CREATE INDEX IF NOT EXISTS idx_summary_company 
    ON company_pageview_summary(company);

CREATE INDEX IF NOT EXISTS idx_summary_total_views 
    ON company_pageview_summary(total_views DESC);

COMMENT ON TABLE company_pageview_summary IS 
    'Daily aggregated analytics by tech company.
    Powers dashboards and ranking reports.';


-- ==========================================
-- DATA QUALITY METRICS VIEW
-- Purpose: Monitor pipeline health and data quality
-- ==========================================

CREATE OR REPLACE VIEW v_data_quality_metrics AS
SELECT 
    processing_date,
    COUNT(*) as raw_records,
    COALESCE((
        SELECT COUNT(*) 
        FROM filtered_pageviews f 
        WHERE f.processing_date = r.processing_date
    ), 0) as filtered_records,
    ROUND(
        100.0 * COALESCE((
            SELECT COUNT(*) 
            FROM filtered_pageviews f 
            WHERE f.processing_date = r.processing_date
        ), 0) / NULLIF(COUNT(*), 0), 
        2
    ) as filter_rate_pct,
    MIN(loaded_at) as first_loaded,
    MAX(loaded_at) as last_loaded,
    COUNT(DISTINCT source_file) as source_files
FROM raw_pageviews r
GROUP BY processing_date
ORDER BY processing_date DESC;

COMMENT ON VIEW v_data_quality_metrics IS 
    'Data quality KPIs: raw vs filtered counts, filter rate, processing times';