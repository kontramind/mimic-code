-- ===============================================================================
-- MIMIC-III DuckDB: Closest NT-proBNP per ICU Stay
-- ===============================================================================
-- This query creates a table with the closest NT-proBNP (N-terminal pro-B-type
-- natriuretic peptide) measurement to ICU admission for each ICU stay in the
-- MIMIC-III database, optimized for DuckDB.
--
-- The table includes:
--   - Closest NT-proBNP value (pg/mL)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source)
--   - Minutes from ICU admission to measurement (can be negative if before)
--
-- ITEMID Configuration:
--   To modify which ITEMIDs are considered for NT-proBNP, edit the list
--   in the WHERE clause within the ntprobnp_measurements CTE below.
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - No filters by default - includes all ICU stays for maximum flexibility
--   - Tracks ITEMID to identify measurement source
--   - NULL values indicate no NT-proBNP measurement during hospital stay
--   - Uses labevents table (laboratory values, not vital signs)
--   - Part of cardiac biomarker series (see also: Troponin, CK-MB tables)
--
-- Unit of Analysis: ICU stays (icustay_id)
--   - Each ICU stay is analyzed independently
--   - One patient can have multiple hospital admissions
--   - One hospital admission can have multiple ICU stays
--   - We find the "closest" measurement to EACH ICU stay's admission time
--
-- DECISION: Time Window from "intime - 7 days" to "outtime" (ICU Stay Bounded)
--   NT-proBNP is a SPARSE CARDIAC BIOMARKER, measured infrequently when heart
--   failure is suspected. We use a bounded window specific to each ICU stay:
--
--   Rationale:
--   - We want the ICU admission baseline (or recent preceding value)
--   - Start: intime - 7 days captures recent measurements (ED, floor, outpatient)
--   - End: outtime ensures we only capture labs from THIS ICU stay, preventing contamination
--   - Without outtime bound, joining on subject_id could capture labs from future ICU stays
--   - This provides clean temporal boundaries for each ICU episode
--   - 7 days is clinically appropriate (NT-proBNP relatively stable over days)
--
--   Implementation:
--   - Join on subject_id (patient level - captures all measurements)
--   - Order by absolute distance from intime (closest = smallest |time difference|)
--   - Bounded to [intime - 7d, outtime] to prevent inter-stay contamination
--   - Time offset exposed in output (can be negative if measured before ICU)
--
-- CLINICAL CONTEXT:
--   NT-proBNP is a cardiac biomarker used to diagnose and assess heart failure.
--   It is released when the heart's ventricles are stretched due to increased
--   volume or pressure, which occurs in heart failure.
--
--   Part of cardiac biomarker panel but NOT routinely measured in ICU.
--
--   Typical use cases in ICU:
--   - Suspected acute decompensated heart failure
--   - Dyspnea evaluation (differentiating cardiac vs. pulmonary causes)
--   - Risk stratification in acute coronary syndrome
--   - Monitoring known heart failure patients
--
--   Note: NT-proBNP can be elevated in conditions other than heart failure:
--   - Renal dysfunction (decreased clearance)
--   - Pulmonary embolism
--   - Sepsis
--   - Advanced age
--
--   Reference Ranges (age-dependent, general guidelines):
--   - Unlikely heart failure: < 300 pg/mL
--   - Borderline: 300-450 pg/mL
--   - Likely heart failure: > 450 pg/mL
--   - Severe heart failure: can reach 35,000+ pg/mL
-- ===============================================================================

DROP TABLE IF EXISTS icu_first_ntprobnp;

CREATE TABLE icu_first_ntprobnp AS
WITH ntprobnp_measurements AS (
    -- Extract all NT-proBNP measurements, ordered by distance from ICU admission
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
        50963       -- NTproBNP (pg/mL) - N-terminal pro-B-type natriuretic peptide
        -- =====================================================================
    )
    -- =========================================================================
    -- VALUE RANGE FILTERS - Edit these thresholds as needed
    -- =========================================================================
    AND le.valuenum > 0         -- Lower limit: lab values must be positive
    AND le.valuenum <= 70000    -- Upper limit: captures severe heart failure, excludes errors
    -- Normal range: < 300 pg/mL typically (age-dependent)
    -- Values > 70,000 pg/mL are extremely rare and likely data entry errors
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

    -- Closest NT-proBNP measurement (within ICU stay bounded window)
    nm.valuenum AS ntprobnp_first,
    nm.charttime AS ntprobnp_first_charttime,
    nm.itemid AS ntprobnp_first_itemid,

    -- Time from ICU admission to measurement (in minutes)
    -- Can be NEGATIVE (measured before ICU admission, within -7d window)
    -- or POSITIVE (measured during ICU stay)
    nm.seconds_from_intime / 60.0 AS ntprobnp_first_minutes_from_intime

FROM icustays ie
LEFT JOIN ntprobnp_measurements nm
    ON ie.icustay_id = nm.icustay_id
    AND nm.rn = 1  -- Only the closest measurement (by absolute time distance)
ORDER BY ie.icustay_id;


-- ===============================================================================
-- DIAGNOSTIC QUERIES
-- ===============================================================================
-- These queries help assess data completeness and quality.
-- Uncomment and run as needed for data validation.

-- Check data completeness: How many ICU stays have NT-proBNP measurements?
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     SUM(CASE WHEN ntprobnp_first IS NOT NULL THEN 1 ELSE 0 END) AS stays_with_ntprobnp,
--     ROUND(100.0 * SUM(CASE WHEN ntprobnp_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_ntprobnp
-- FROM icu_first_ntprobnp;

-- Distribution of NT-proBNP values
-- SELECT
--     MIN(ntprobnp_first) AS min_ntprobnp,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ntprobnp_first) AS p25_ntprobnp,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ntprobnp_first) AS median_ntprobnp,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ntprobnp_first) AS p75_ntprobnp,
--     MAX(ntprobnp_first) AS max_ntprobnp
-- FROM icu_first_ntprobnp
-- WHERE ntprobnp_first IS NOT NULL;

-- Check ITEMID distribution (should only be 50963 unless ITEMIDs are modified)
-- SELECT
--     ntprobnp_first_itemid,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_ntprobnp
-- WHERE ntprobnp_first_itemid IS NOT NULL
-- GROUP BY ntprobnp_first_itemid
-- ORDER BY n_stays DESC;

-- Timing analysis: When are closest NT-proBNP measurements typically taken?
-- Note: This shows the temporal distribution relative to ICU admission
-- Negative values = measured BEFORE ICU admission (within 7 days - ED, floor, outpatient)
-- Positive values = measured AFTER ICU admission (during ICU stay)
-- All measurements are within [intime - 7d, outtime] window
-- SELECT
--     CASE
--         WHEN ntprobnp_first_minutes_from_intime < -4320 THEN 'More than 3 days before ICU'
--         WHEN ntprobnp_first_minutes_from_intime < -1440 THEN '1-3 days before ICU'
--         WHEN ntprobnp_first_minutes_from_intime < -360 THEN '6-24 hours before ICU'
--         WHEN ntprobnp_first_minutes_from_intime < 0 THEN 'Within 6h before ICU'
--         WHEN ntprobnp_first_minutes_from_intime <= 360 THEN 'Within 6h after ICU'
--         WHEN ntprobnp_first_minutes_from_intime <= 1440 THEN '6-24 hours after ICU'
--         WHEN ntprobnp_first_minutes_from_intime <= 4320 THEN '1-3 days after ICU'
--         ELSE 'More than 3 days after ICU'
--     END AS timing_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_ntprobnp
-- WHERE ntprobnp_first IS NOT NULL
-- GROUP BY timing_category
-- ORDER BY MIN(ntprobnp_first_minutes_from_intime);

-- Check NT-proBNP levels (heart failure assessment)
-- Clinical cutoffs for heart failure diagnosis
-- SELECT
--     CASE
--         WHEN ntprobnp_first < 300 THEN 'Normal/Unlikely HF (< 300)'
--         WHEN ntprobnp_first < 450 THEN 'Borderline (300-450)'
--         WHEN ntprobnp_first < 900 THEN 'Mild Elevation (450-900)'
--         WHEN ntprobnp_first < 1800 THEN 'Moderate Elevation (900-1800)'
--         WHEN ntprobnp_first < 5000 THEN 'Severe Elevation (1800-5000)'
--         ELSE 'Very Severe Elevation (> 5000)'
--     END AS ntprobnp_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_ntprobnp
-- WHERE ntprobnp_first IS NOT NULL
-- GROUP BY ntprobnp_category
-- ORDER BY MIN(ntprobnp_first);
