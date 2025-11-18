-- ===============================================================================
-- MIMIC-III DuckDB: First Creatinine per ICU Stay
-- ===============================================================================
-- This query creates a table with the first creatinine measurement for each
-- ICU stay in the MIMIC-III database, optimized for DuckDB.
--
-- The table includes:
--   - First creatinine value (mg/dL)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source)
--   - Minutes from ICU admission to measurement
--
-- ITEMID Configuration:
--   To modify which ITEMIDs are considered for creatinine, edit the list
--   in the WHERE clause within the creat_measurements CTE below.
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - No filters by default - includes all ICU stays for maximum flexibility
--   - Tracks ITEMID to identify measurement source
--   - NULL values indicate no creatinine measurement during the defined time window
--   - Uses labevents table (laboratory values, not vital signs)
--   - Part of routine chemistry panel (see also: BUN, Potassium tables)
--
-- Unit of Analysis: ICU stays (icustay_id)
--   - Each ICU stay is analyzed independently
--   - One patient can have multiple hospital admissions
--   - One hospital admission can have multiple ICU stays
--   - We find the "first" measurement for EACH ICU stay
--
-- DECISION: Time Window from "intime - 6 hours" to "outtime" (ICU Stay Bounded)
--   Creatinine is a ROUTINE lab, measured frequently in ICU (~800k measurements).
--   We use a bounded window specific to each ICU stay:
--
--   Rationale:
--   - We want the ICU admission baseline (or immediately preceding value)
--   - Start: intime - 6 hours captures pre-ICU labs (ED, floor) that reflect admission state
--   - End: outtime ensures we only capture labs from THIS ICU stay, preventing contamination
--   - Without outtime bound, joining on subject_id could capture labs from future ICU stays
--   - This provides clean temporal boundaries for each ICU episode
--
--   Implementation:
--   - Join on subject_id (routine lab approach)
--   - Order by charttime (first chronologically within window)
--   - Bounded to [intime - 6h, outtime] to prevent inter-stay contamination
-- ===============================================================================

DROP TABLE IF EXISTS icu_first_creatinine;

CREATE TABLE icu_first_creatinine AS
WITH creat_measurements AS (
    -- Extract all creatinine measurements with temporal ordering per ICU stay
    SELECT
        ie.icustay_id,
        le.charttime,
        le.itemid,
        le.valuenum,
        ROW_NUMBER() OVER (PARTITION BY ie.icustay_id ORDER BY le.charttime) AS rn
    FROM icustays ie
    INNER JOIN labevents le
        ON ie.subject_id = le.subject_id
    WHERE le.itemid IN (
        -- =====================================================================
        -- EDIT THIS LIST to configure which ITEMIDs to include
        -- =====================================================================
        50912       -- Creatinine (mg/dL)
        -- =====================================================================
    )
    -- =========================================================================
    -- VALUE RANGE FILTERS - Edit these thresholds as needed
    -- =========================================================================
    AND le.valuenum > 0         -- Lower limit: lab values must be positive
    AND le.valuenum <= 150      -- Upper limit: physiologically plausible (mg/dL)
    -- =========================================================================

    -- =========================================================================
    -- TIME WINDOW - Edit to adjust temporal filtering
    -- =========================================================================
    -- IMPORTANT: Bounded to specific ICU stay to prevent contamination
    AND le.charttime >= ie.intime - INTERVAL '6' HOUR  -- Start: 6h before ICU admission
    AND le.charttime <= ie.outtime                     -- End: ICU discharge (prevents future stay contamination)
    -- This ensures we capture:
    --   1. Pre-ICU labs from ED/floor (admission baseline)
    --   2. Labs during ICU stay
    --   3. ONLY from THIS specific ICU stay (no contamination from future stays)
    -- =========================================================================
)
SELECT
    ie.icustay_id,
    ie.subject_id,
    ie.hadm_id,
    ie.intime AS icu_intime,
    ie.outtime AS icu_outtime,

    -- First creatinine measurement (within ICU stay bounded window)
    cm.valuenum AS creatinine_first,
    cm.charttime AS creatinine_first_charttime,
    cm.itemid AS creatinine_first_itemid,

    -- Time from ICU admission to measurement (in minutes)
    -- Can be NEGATIVE (measured before ICU admission, within -6h window)
    -- or POSITIVE (measured during ICU stay)
    DATE_DIFF('second', ie.intime, cm.charttime) / 60.0 AS creatinine_first_minutes_from_intime

FROM icustays ie
LEFT JOIN creat_measurements cm
    ON ie.icustay_id = cm.icustay_id
    AND cm.rn = 1  -- Only the first measurement
ORDER BY ie.icustay_id;


-- ===============================================================================
-- DIAGNOSTIC QUERIES
-- ===============================================================================
-- These queries help assess data completeness and quality.
-- Uncomment and run as needed for data validation.

-- Check data completeness: How many ICU stays have creatinine measurements?
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     SUM(CASE WHEN creatinine_first IS NOT NULL THEN 1 ELSE 0 END) AS stays_with_creatinine,
--     ROUND(100.0 * SUM(CASE WHEN creatinine_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_creatinine
-- FROM icu_first_creatinine;

-- Distribution of creatinine values
-- SELECT
--     MIN(creatinine_first) AS min_creat,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY creatinine_first) AS p25_creat,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY creatinine_first) AS median_creat,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY creatinine_first) AS p75_creat,
--     MAX(creatinine_first) AS max_creat
-- FROM icu_first_creatinine
-- WHERE creatinine_first IS NOT NULL;

-- Check ITEMID distribution: Which ITEMIDs are most common?
-- SELECT
--     creatinine_first_itemid,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_creatinine
-- WHERE creatinine_first_itemid IS NOT NULL
-- GROUP BY creatinine_first_itemid
-- ORDER BY n_stays DESC;

-- Timing analysis: When are first creatinine measurements typically taken?
-- SELECT
--     CASE
--         WHEN creatinine_first_minutes_from_intime < -60 THEN 'More than 1h before admission'
--         WHEN creatinine_first_minutes_from_intime < 0 THEN 'Within 1h before admission'
--         WHEN creatinine_first_minutes_from_intime <= 60 THEN 'Within 1h after admission'
--         WHEN creatinine_first_minutes_from_intime <= 360 THEN 'Within 6h after admission'
--         WHEN creatinine_first_minutes_from_intime <= 1440 THEN 'Within 24h after admission'
--         ELSE 'After 24h'
--     END AS timing_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_creatinine
-- WHERE creatinine_first IS NOT NULL
-- GROUP BY timing_category
-- ORDER BY MIN(creatinine_first_minutes_from_intime);

-- Check for elevated creatinine (potential AKI indicators)
-- Normal creatinine: 0.6-1.2 mg/dL (varies by gender/age)
-- SELECT
--     CASE
--         WHEN creatinine_first < 0.6 THEN 'Low (< 0.6)'
--         WHEN creatinine_first <= 1.2 THEN 'Normal (0.6-1.2)'
--         WHEN creatinine_first <= 2.0 THEN 'Mildly Elevated (1.2-2.0)'
--         WHEN creatinine_first <= 4.0 THEN 'Moderately Elevated (2.0-4.0)'
--         ELSE 'Severely Elevated (> 4.0)'
--     END AS creat_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_creatinine
-- WHERE creatinine_first IS NOT NULL
-- GROUP BY creat_category
-- ORDER BY MIN(creatinine_first);
