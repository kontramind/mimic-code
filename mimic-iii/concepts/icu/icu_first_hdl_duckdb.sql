-- ===============================================================================
-- MIMIC-III DuckDB: Closest HDL Cholesterol per ICU Stay
-- ===============================================================================
-- This query creates a table with the closest HDL cholesterol measurement to
-- ICU admission for each ICU stay in the MIMIC-III database, optimized for DuckDB.
--
-- The table includes:
--   - Closest HDL cholesterol value (mg/dL)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source)
--   - Minutes from ICU admission to measurement (can be negative if before)
--
-- ITEMID Configuration:
--   To modify which ITEMIDs are considered for HDL cholesterol, edit the list
--   in the WHERE clause within the hdl_measurements CTE below.
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - No filters by default - includes all ICU stays for maximum flexibility
--   - Tracks ITEMID to identify measurement source
--   - NULL values indicate no HDL cholesterol measurement during hospital stay
--   - Uses labevents table (laboratory values, not vital signs)
--   - Part of lipid panel series (see also: Total Chol, LDL, triglycerides tables)
--
-- Unit of Analysis: ICU stays (icustay_id)
--   - Each ICU stay is analyzed independently
--   - One patient can have multiple hospital admissions
--   - One hospital admission can have multiple ICU stays
--   - We find the "closest" measurement to EACH ICU stay's admission time
--
-- DECISION: Time Window from "intime - 7 days" to "outtime" (ICU Stay Bounded)
--   HDL is a SPARSE LAB, measured infrequently as part of lipid panel.
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
--   HDL (High-Density Lipoprotein) cholesterol is known as "good" cholesterol.
--   It helps remove other forms of cholesterol from the bloodstream.
--   Higher HDL levels are associated with lower cardiovascular risk.
--
--   Part of standard lipid panel but NOT routinely measured in ICU.
--
--   Typical use cases in ICU:
--   - Cardiovascular risk assessment
--   - Pre-existing dyslipidemia management
--   - Post-cardiac event monitoring
--
--   Note: Critical illness can affect HDL levels (acute phase response).
--   HDL often decreases during acute illness.
--
--   Reference Ranges:
--   - Low risk (desirable): ≥60 mg/dL
--   - Normal: 40-59 mg/dL (men), 50-59 mg/dL (women)
--   - Increased cardiovascular risk: <40 mg/dL (men), <50 mg/dL (women)
-- ===============================================================================

DROP TABLE IF EXISTS icu_first_hdl;

CREATE TABLE icu_first_hdl AS
WITH hdl_measurements AS (
    -- Extract all HDL cholesterol measurements, ordered by distance from ICU admission
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
        50904       -- Cholesterol, HDL (mg/dL) - ~256k measurements
        -- =====================================================================
    )
    -- =========================================================================
    -- VALUE RANGE FILTERS - Edit these thresholds as needed
    -- =========================================================================
    AND le.valuenum > 0         -- Lower limit: lab values must be positive
    AND le.valuenum <= 150      -- Upper limit: filters extreme outliers (mg/dL)
    -- Normal range: 40-60 mg/dL typically
    -- Values >150 mg/dL are extremely rare and likely data entry errors
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

    -- Closest HDL cholesterol measurement (within hospital admission)
    hm.valuenum AS hdl_first,
    hm.charttime AS hdl_first_charttime,
    hm.itemid AS hdl_first_itemid,

    -- Time from ICU admission to measurement (in minutes)
    -- IMPORTANT: Can be NEGATIVE (measured before ICU) or POSITIVE (measured after ICU)
    -- This is different from routine labs which are typically within ±6 hours
    hm.seconds_from_intime / 60.0 AS hdl_first_minutes_from_intime

FROM icustays ie
LEFT JOIN hdl_measurements hm
    ON ie.icustay_id = hm.icustay_id
    AND hm.rn = 1  -- Only the closest measurement (by absolute time distance)
ORDER BY ie.icustay_id;


-- ===============================================================================
-- DIAGNOSTIC QUERIES
-- ===============================================================================
-- These queries help assess data completeness and quality.
-- Uncomment and run as needed for data validation.

-- Check data completeness: How many ICU stays have HDL measurements?
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     SUM(CASE WHEN hdl_first IS NOT NULL THEN 1 ELSE 0 END) AS stays_with_hdl,
--     ROUND(100.0 * SUM(CASE WHEN hdl_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_hdl
-- FROM icu_first_hdl;

-- Distribution of HDL values
-- SELECT
--     MIN(hdl_first) AS min_hdl,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY hdl_first) AS p25_hdl,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY hdl_first) AS median_hdl,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY hdl_first) AS p75_hdl,
--     MAX(hdl_first) AS max_hdl
-- FROM icu_first_hdl
-- WHERE hdl_first IS NOT NULL;

-- Check ITEMID distribution (should only be 50904 unless ITEMIDs are modified)
-- SELECT
--     hdl_first_itemid,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_hdl
-- WHERE hdl_first_itemid IS NOT NULL
-- GROUP BY hdl_first_itemid
-- ORDER BY n_stays DESC;

-- Timing analysis: When are closest HDL measurements typically taken?
-- Note: This shows the temporal distribution relative to ICU admission
-- Negative values = measured BEFORE ICU admission
-- SELECT
--     CASE
--         WHEN hdl_first_minutes_from_intime < -4320 THEN 'More than 3 days before ICU'
--         WHEN hdl_first_minutes_from_intime < -1440 THEN '1-3 days before ICU'
--         WHEN hdl_first_minutes_from_intime < -360 THEN '6-24 hours before ICU'
--         WHEN hdl_first_minutes_from_intime < 0 THEN 'Within 6h before ICU'
--         WHEN hdl_first_minutes_from_intime <= 360 THEN 'Within 6h after ICU'
--         WHEN hdl_first_minutes_from_intime <= 1440 THEN '6-24 hours after ICU'
--         WHEN hdl_first_minutes_from_intime <= 4320 THEN '1-3 days after ICU'
--         ELSE 'More than 3 days after ICU'
--     END AS timing_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*) OVER (), 2) AS pct
-- FROM icu_first_hdl
-- WHERE hdl_first IS NOT NULL
-- GROUP BY timing_category
-- ORDER BY MIN(hdl_first_minutes_from_intime);

-- Check HDL level categories (cardiovascular risk assessment)
-- SELECT
--     CASE
--         WHEN hdl_first < 40 THEN 'Low (Increased CVD Risk) (< 40)'
--         WHEN hdl_first < 60 THEN 'Normal (40-59)'
--         WHEN hdl_first < 80 THEN 'High (Cardioprotective) (60-79)'
--         ELSE 'Very High (≥ 80)'
--     END AS hdl_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_hdl
-- WHERE hdl_first IS NOT NULL
-- GROUP BY hdl_category
-- ORDER BY MIN(hdl_first);
