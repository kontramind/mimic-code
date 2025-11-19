-- ===============================================================================
-- MIMIC-III DuckDB: Closest Total Cholesterol per ICU Stay
-- ===============================================================================
-- This query creates a table with the closest total cholesterol measurement to
-- ICU admission for each ICU stay in the MIMIC-III database, optimized for DuckDB.
--
-- The table includes:
--   - Closest total cholesterol value (mg/dL)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source)
--   - Minutes from ICU admission to measurement (can be negative if before)
--
-- ITEMID Configuration:
--   To modify which ITEMIDs are considered for total cholesterol, edit the list
--   in the WHERE clause within the chol_measurements CTE below.
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - No filters by default - includes all ICU stays for maximum flexibility
--   - Tracks ITEMID to identify measurement source
--   - NULL values indicate no total cholesterol measurement during hospital stay
--   - Uses labevents table (laboratory values, not vital signs)
--   - Part of lipid panel series (see also: HDL, LDL, triglycerides tables)
--
-- Unit of Analysis: ICU stays (icustay_id)
--   - Each ICU stay is analyzed independently
--   - One patient can have multiple hospital admissions
--   - One hospital admission can have multiple ICU stays
--   - We find the "closest" measurement to EACH ICU stay's admission time
--
-- DECISION: Time Window from "intime - 7 days" to "outtime" (ICU Stay Bounded)
--   Total Cholesterol is a SPARSE LAB, measured infrequently as part of lipid panel.
--   We use a bounded window specific to each ICU stay:
--
--   Rationale:
--   - We want the ICU admission baseline (or recent preceding value)
--   - Start: intime - 7 days captures recent measurements (ED, floor, outpatient)
--   - End: outtime ensures we only capture labs from THIS ICU stay, preventing contamination
--   - Without outtime bound, joining on subject_id could capture labs from future ICU stays
--   - This provides clean temporal boundaries for each ICU episode
--   - 7 days is clinically appropriate (lipids relatively stable over days)
--
--   Implementation:
--   - Join on subject_id (patient level - captures all measurements)
--   - Order by absolute distance from intime (closest = smallest |time difference|)
--   - Bounded to [intime - 7d, outtime] to prevent inter-stay contamination
--   - Time offset exposed in output (can be negative if measured before ICU)
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
    -- Extract all total cholesterol measurements, ordered by distance from ICU admission
    SELECT
        ie.icustay_id,
        le.charttime,
        le.itemid,
        le.valuenum,
        -- Calculate time difference from ICU admission (can be negative if before)
        DATE_DIFF('second', ie.intime, le.charttime) AS seconds_from_intime,
        ROW_NUMBER() OVER (
            PARTITION BY ie.icustay_id    -- Each ICU stay analyzed independently
            ORDER BY ABS(DATE_DIFF('second', ie.intime, le.charttime))  -- Closest by absolute distance
        ) AS rn
    FROM icustays ie
    INNER JOIN labevents le
        ON ie.subject_id = le.subject_id  -- Join at PATIENT level (all measurements)
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
    -- IMPORTANT: Bounded to specific ICU stay to prevent contamination
    AND le.charttime >= ie.intime - INTERVAL '7' DAY  -- Start: 7 days before ICU admission
    AND le.charttime <= ie.outtime                    -- End: ICU discharge (prevents future stay contamination)
    -- This ensures we capture:
    --   1. Recent pre-ICU measurements (ED, floor, outpatient within 7 days)
    --   2. Measurements during ICU stay
    --   3. ONLY from THIS specific ICU stay (no contamination from future stays)
    -- =========================================================================
)
SELECT
    ie.icustay_id,
    ie.subject_id,
    ie.hadm_id,
    ie.intime AS icu_intime,

    -- Closest total cholesterol measurement (within hospital admission)
    cm.valuenum AS total_cholesterol_first,
    cm.charttime AS total_cholesterol_first_charttime,
    cm.itemid AS total_cholesterol_first_itemid,

    -- Time from ICU admission to measurement (in minutes)
    -- IMPORTANT: Can be NEGATIVE (measured before ICU) or POSITIVE (measured after ICU)
    -- This is different from routine labs which are typically within ±6 hours
    cm.seconds_from_intime / 60.0 AS total_cholesterol_first_minutes_from_intime

FROM icustays ie
LEFT JOIN chol_measurements cm
    ON ie.icustay_id = cm.icustay_id
    AND cm.rn = 1  -- Only the closest measurement (by absolute time distance)
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

-- Timing analysis: When are closest total cholesterol measurements typically taken?
-- Note: This shows the temporal distribution relative to ICU admission
-- Negative values = measured BEFORE ICU admission
-- SELECT
--     CASE
--         WHEN total_cholesterol_first_minutes_from_intime < -4320 THEN 'More than 3 days before ICU'
--         WHEN total_cholesterol_first_minutes_from_intime < -1440 THEN '1-3 days before ICU'
--         WHEN total_cholesterol_first_minutes_from_intime < -360 THEN '6-24 hours before ICU'
--         WHEN total_cholesterol_first_minutes_from_intime < 0 THEN 'Within 6h before ICU'
--         WHEN total_cholesterol_first_minutes_from_intime <= 360 THEN 'Within 6h after ICU'
--         WHEN total_cholesterol_first_minutes_from_intime <= 1440 THEN '6-24 hours after ICU'
--         WHEN total_cholesterol_first_minutes_from_intime <= 4320 THEN '1-3 days after ICU'
--         ELSE 'More than 3 days after ICU'
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
