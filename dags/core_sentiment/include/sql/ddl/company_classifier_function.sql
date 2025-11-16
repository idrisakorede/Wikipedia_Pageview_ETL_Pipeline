-- =====================================================================
-- ================== COMPANY CLASSIFICATION FUNCTION ==================
-- ======== Purpose: Classify Wikipedia pages to tech companies ========
-- ======= Business Logic: Match page titles to company keywords =======
-- =====================================================================

CREATE OR REPLACE FUNCTION classify_company(page_title TEXT)
RETURNS VARCHAR(50) AS $$
DECLARE
    title_lower TEXT;
BEGIN
    title_lower := LOWER(page_title);
    
    -- Amazon products and services
    IF title_lower LIKE '%amazon%'
        OR title_lower LIKE '%aws%'
        OR title_lower LIKE '%alexa%'
        OR title_lower LIKE '%kindle%'
        OR title_lower LIKE '%prime_video%'
        OR title_lower LIKE '%fire_tv%'
        OR title_lower LIKE '%echo_%'
    THEN
        RETURN 'Amazon';
    END IF;
    
    -- Apple products and services
    IF title_lower LIKE '%apple%'
        OR title_lower LIKE '%iphone%'
        OR title_lower LIKE '%ipad%'
        OR title_lower LIKE '%macbook%'
        OR title_lower LIKE '%imac%'
        OR title_lower LIKE '%ios%'
        OR title_lower LIKE '%macos%'
        OR title_lower LIKE '%airpods%'
        OR title_lower LIKE '%apple_watch%'
        OR title_lower LIKE '%app_store%'
        OR title_lower LIKE '%itunes%'
    THEN
        RETURN 'Apple';
    END IF;
    
    -- Google products and services
    IF title_lower LIKE '%google%'
        OR title_lower LIKE '%android%'
        OR title_lower LIKE '%chrome%'
        OR title_lower LIKE '%youtube%'
        OR title_lower LIKE '%gmail%'
        OR title_lower LIKE '%pixel%'
        OR title_lower LIKE '%google_maps%'
        OR title_lower LIKE '%google_drive%'
        OR title_lower LIKE '%google_cloud%'
        OR title_lower LIKE '%nest_%'
    THEN
        RETURN 'Google';
    END IF;
    
    -- Microsoft products and services
    IF title_lower LIKE '%microsoft%'
        OR title_lower LIKE '%windows%'
        OR title_lower LIKE '%xbox%'
        OR title_lower LIKE '%azure%'
        OR title_lower LIKE '%office_365%'
        OR title_lower LIKE '%teams%'
        OR title_lower LIKE '%outlook%'
        OR title_lower LIKE '%surface%'
        OR title_lower LIKE '%bing%'
    THEN
        RETURN 'Microsoft';
    END IF;
    
    -- Meta (Facebook) products and services
    IF title_lower LIKE '%facebook%'
        OR title_lower LIKE '%meta%'
        OR title_lower LIKE '%instagram%'
        OR title_lower LIKE '%whatsapp%'
        OR title_lower LIKE '%oculus%'
        OR title_lower LIKE '%messenger%'
        OR title_lower LIKE '%threads%'
    THEN
        RETURN 'Meta';
    END IF;
    
    -- No match
    RETURN 'Other';
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION classify_company IS 
    'Classifies Wikipedia page titles to tech companies (Amazon, Apple, Google, Microsoft, Meta).
    Returns ''Other'' if no match found.
    Used in analytics and reporting queries.';