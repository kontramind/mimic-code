-- ===============================================================================
-- MIMIC-III DuckDB: First Total Cholesterol per ICU Stay
-- ===============================================================================
-- This query creates a table with the first total cholesterol measurement for
-- each ICU stay in the MIMIC-III database, optimized for DuckDB.
--
-- The table includes:
--   - First total cholesterol value (mg/dL)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source)
--   - Minutes from ICU admission to measurement
--
-- ITEMID Configuration:
--   To modify which ITEMIDs are considered for total cholesterol, edit the list
--   in the WHERE clause within the chol_measurements CTE below.
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - No filters by default - includes all ICU stays for maximum flexibility
--   - Tracks ITEMID to identify measurement source
--   - NULL values indicate no total cholesterol measurement during ICU stay
--   - Allows measurements from 6 hours BEFORE admission (captures baseline labs)
--   - Uses labevents table (laboratory values, not vital signs)
--   - Part of lipid panel series (see also: HDL, LDL, triglycerides tables)
--
-- Unit of Analysis: ICU stays (icustay_id)
--   - Each ICU stay is analyzed independently
--   - One patient can have multiple hospital admissions
--   - One hospital admission can have multiple ICU stays
--   - We find the "first" measurement relative to EACH ICU stay's admission time
--
-- CLINICAL CONTEXT:
--   Total cholesterol is the sum of all cholesterol components (HDL, LDL, VLDL).
--   It is part of a standard lipid panel but NOT routinely measured in ICU.
--
--   Typical use cases in ICU:
--   - Cardiovascular patients
--   - Pre-existing dyslipidemia management
--   - Nutritional assessment
--
--   Note: Critical illness can affect cholesterol levels (acute phase response).
--   Baseline ICU admission values may not reflect true baseline.
--
--   Reference Ranges:
--   - Desirable: <200 mg/dL
--   - Borderline high: 200-239 mg/dL
--   - High: ≥240 mg/dL
-- ===============================================================================

DROP TABLE IF EXISTS icu_first_total_cholesterol;

CREATE TABLE icu_first_total_cholesterol AS
WITH chol_measurements AS (
    -- Extract all total cholesterol measurements with temporal ordering per ICU stay
    SELECT
        ie.icustay_id,
        le.charttime,
        le.itemid,
        le.valuenum,
        ROW_NUMBER() OVER (
            PARTITION BY ie.icustay_id    -- Each ICU stay analyzed independently
            ORDER BY le.charttime          -- Earliest by time
        ) AS rn
    FROM icustays ie
    INNER JOIN labevents le
        ON ie.subject_id = le.subject_id
    WHERE le.itemid IN (
        -- =====================================================================
        -- EDIT THIS LIST to configure which ITEMIDs to include
        -- =====================================================================
        50907       -- Cholesterol, Total (mg/dL) - ~262k measurements
        -- =====================================================================
    )
    -- =========================================================================
    -- VALUE RANGE FILTERS - Edit these thresholds as needed
    -- =========================================================================
    AND le.valuenum > 0         -- Lower limit: lab values must be positive
    AND le.valuenum <= 500      -- Upper limit: filters extreme outliers (mg/dL)
    -- Normal range: <200 mg/dL (desirable)
    -- Values >500 mg/dL are extremely rare and likely data entry errors
    -- =========================================================================

    -- =========================================================================
    -- TIME WINDOW - Edit to adjust temporal filtering
    -- =========================================================================
    -- Allow measurements from 6 hours BEFORE ICU admission to capture baseline
    -- This is clinically relevant as labs are often drawn pre-admission
    AND le.charttime >= ie.intime - INTERVAL '6' HOUR
    -- =========================================================================
)
SELECT
    ie.icustay_id,
    ie.subject_id,
    ie.hadm_id,
    ie.intime AS icu_intime,

    -- First total cholesterol measurement
    cm.valuenum AS total_cholesterol_first,
    cm.charttime AS total_cholesterol_first_charttime,
    cm.itemid AS total_cholesterol_first_itemid,

    -- Time from ICU admission to measurement (in minutes)
    -- Note: Can be NEGATIVE if measurement was taken before admission
    DATE_DIFF('second', ie.intime, cm.charttime) / 60.0 AS total_cholesterol_first_minutes_from_intime

FROM icustays ie
LEFT JOIN chol_measurements cm
    ON ie.icustay_id = cm.icustay_id
    AND cm.rn = 1  -- Only the first measurement (earliest by charttime)
ORDER BY ie.icustay_id;


-- ===============================================================================
-- DIAGNOSTIC QUERIES
-- ===============================================================================
-- These queries help assess data completeness and quality.
-- Uncomment and run as needed for data validation.

-- Check data completeness: How many ICU stays have total cholesterol measurements?
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     SUM(CASE WHEN total_cholesterol_first IS NOT NULL THEN 1 ELSE 0 END) AS stays_with_chol,
--     ROUND(100.0 * SUM(CASE WHEN total_cholesterol_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_chol
-- FROM icu_first_total_cholesterol;

-- Distribution of total cholesterol values
-- SELECT
--     MIN(total_cholesterol_first) AS min_chol,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_cholesterol_first) AS p25_chol,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_cholesterol_first) AS median_chol,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_cholesterol_first) AS p75_chol,
--     MAX(total_cholesterol_first) AS max_chol
-- FROM icu_first_total_cholesterol
-- WHERE total_cholesterol_first IS NOT NULL;

-- Check ITEMID distribution (should only be 50907 unless ITEMIDs are modified)
-- SELECT
--     total_cholesterol_first_itemid,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_total_cholesterol
-- WHERE total_cholesterol_first_itemid IS NOT NULL
-- GROUP BY total_cholesterol_first_itemid
-- ORDER BY n_stays DESC;

-- Timing analysis: When are first total cholesterol measurements typically taken?
-- SELECT
--     CASE
--         WHEN total_cholesterol_first_minutes_from_intime < -60 THEN 'More than 1h before admission'
--         WHEN total_cholesterol_first_minutes_from_intime < 0 THEN 'Within 1h before admission'
--         WHEN total_cholesterol_first_minutes_from_intime <= 60 THEN 'Within 1h after admission'
--         WHEN total_cholesterol_first_minutes_from_intime <= 360 THEN 'Within 6h after admission'
--         WHEN total_cholesterol_first_minutes_from_intime <= 1440 THEN 'Within 24h after admission'
--         ELSE 'After 24h'
--     END AS timing_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_total_cholesterol
-- WHERE total_cholesterol_first IS NOT NULL
-- GROUP BY timing_category
-- ORDER BY MIN(total_cholesterol_first_minutes_from_intime);

-- Check cholesterol level categories (cardiovascular risk assessment)
-- SELECT
--     CASE
--         WHEN total_cholesterol_first < 200 THEN 'Desirable (< 200)'
--         WHEN total_cholesterol_first < 240 THEN 'Borderline High (200-239)'
--         WHEN total_cholesterol_first < 300 THEN 'High (240-299)'
--         ELSE 'Very High (≥ 300)'
--     END AS chol_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_total_cholesterol
-- WHERE total_cholesterol_first IS NOT NULL
-- GROUP BY chol_category
-- ORDER BY MIN(total_cholesterol_first);
