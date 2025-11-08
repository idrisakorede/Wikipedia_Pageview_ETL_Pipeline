CREATE TABLE IF NOT EXISTS company_pageviews (
    id SERIAL PRIMARY KEY,
    domain_name TEXT NOT NULL,
    company_name TEXT NOT NULL,
    pageviews INTEGER NOT NULL CHECK (pageviews >= 0),
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_company_file UNIQUE (company_name, source_file)
    );