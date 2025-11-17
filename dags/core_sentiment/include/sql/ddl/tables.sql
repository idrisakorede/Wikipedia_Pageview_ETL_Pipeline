-- ===================================================================
-- ================== RAW DATA WAREHOUSE TABLES ======================
-- Purpose: Immutable storage of all extracted Wikipedia pageview data
-- ===================================================================

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
    ON raw_pageviews USING gin(page_title gin_trgm_ops);

COMMENT ON TABLE raw_pageviews IS 
    'Warehouse table for all Wikipedia pageviews. 
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
    company VARCHAR(50) NOT NULL,  -- Added: company classification
    filtered_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processing_date DATE NOT NULL,
    filter_method VARCHAR(100) NOT NULL,
    
    CONSTRAINT unique_filtered_pageview 
        UNIQUE (domain, page_title, processing_date, filter_method)
);

CREATE INDEX IF NOT EXISTS idx_filtered_processing_date 
    ON filtered_pageviews(processing_date DESC);

CREATE INDEX IF NOT EXISTS idx_filtered_company 
    ON filtered_pageviews(company);

CREATE INDEX IF NOT EXISTS idx_filtered_views 
    ON filtered_pageviews(count_views DESC);

CREATE INDEX IF NOT EXISTS idx_filtered_company_date 
    ON filtered_pageviews(company, processing_date);

COMMENT ON TABLE filtered_pageviews IS 
    'Curated dataset of LLM-filtered pages with company classification.
    This is the single source of truth for all analytics.';


-- ===================================================================
-- ================= SENTIMENT RESULTS TABLE =========================
-- ======== Purpose: Store LLM sentiment analysis results ===========
-- ===================================================================

CREATE TABLE IF NOT EXISTS sentiment_results (
    id BIGSERIAL PRIMARY KEY,
    page_title TEXT NOT NULL,
    company VARCHAR(50) NOT NULL,
    sentiment_label VARCHAR(20) NOT NULL,  -- positive, neutral, negative
    sentiment_score NUMERIC(4,3),  -- -1.0 to 1.0
    count_views INTEGER NOT NULL,
    processing_date DATE NOT NULL,
    analyzed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_sentiment_page_date 
        UNIQUE (page_title, processing_date),
    CONSTRAINT valid_sentiment_label 
        CHECK (sentiment_label IN ('positive', 'neutral', 'negative'))
);

CREATE INDEX IF NOT EXISTS idx_sentiment_date 
    ON sentiment_results(processing_date DESC);

CREATE INDEX IF NOT EXISTS idx_sentiment_company 
    ON sentiment_results(company);

CREATE INDEX IF NOT EXISTS idx_sentiment_label 
    ON sentiment_results(sentiment_label);

COMMENT ON TABLE sentiment_results IS 
    'LLM-generated sentiment analysis for filtered pageviews.
    Used by Streamlit dashboard for sentiment visualization.';


-- ===================================================================
-- ==================== ANALYTICS VIEWS ==============================
-- ========= Purpose: Pre-defined queries for dashboards =============
-- ===================================================================

-- Daily company summary (replaces company_pageview_summary table)
CREATE OR REPLACE VIEW v_daily_company_summary AS
SELECT 
    company,
    processing_date,
    COUNT(*) as page_count,
    SUM(count_views) as total_views,
    ROUND(AVG(count_views), 2) as avg_views,
    MAX(count_views) as max_views,
    MIN(count_views) as min_views
FROM filtered_pageviews
GROUP BY company, processing_date
ORDER BY processing_date DESC, total_views DESC;

COMMENT ON VIEW v_daily_company_summary IS 
    'Daily aggregated metrics by company. 
    Computed on-the-fly from filtered_pageviews.';


-- Data quality metrics
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
    'Pipeline health KPIs for monitoring dashboard.';


-- Top pages by company (last 7 days)
CREATE OR REPLACE VIEW v_top_pages_by_company AS
SELECT 
    company,
    page_title,
    count_views,
    processing_date,
    ROW_NUMBER() OVER (PARTITION BY company ORDER BY count_views DESC) as rank
FROM filtered_pageviews
WHERE processing_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY company, rank;

COMMENT ON VIEW v_top_pages_by_company IS 
    'Top 10 pages per company for quick dashboard queries.';


-- Sentiment distribution
CREATE OR REPLACE VIEW v_sentiment_distribution AS
SELECT 
    company,
    processing_date,
    sentiment_label,
    COUNT(*) as count,
    ROUND(AVG(sentiment_score), 3) as avg_score,
    SUM(count_views) as total_views
FROM sentiment_results
GROUP BY company, processing_date, sentiment_label
ORDER BY processing_date DESC, company;

COMMENT ON VIEW v_sentiment_distribution IS 
    'Sentiment breakdown by company and date for dashboard charts.';