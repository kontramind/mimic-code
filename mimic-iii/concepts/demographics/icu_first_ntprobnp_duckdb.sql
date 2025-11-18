-- ===============================================================================
-- MIMIC-III DuckDB: First NT-proBNP per ICU Stay
-- ===============================================================================
-- This query creates a table with the first NT-proBNP (N-terminal pro-B-type
-- natriuretic peptide) measurement for each ICU stay in the MIMIC-III database,
-- optimized for DuckDB.
--
-- The table includes:
--   - First NT-proBNP value (pg/mL)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source)
--   - Minutes from ICU admission to measurement
--
-- ITEMID Configuration:
--   To modify which ITEMIDs are considered for NT-proBNP, edit the list
--   in the WHERE clause within the ntprobnp_measurements CTE below.
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - No filters by default - includes all ICU stays for maximum flexibility
--   - Tracks ITEMID to identify measurement source
--   - NULL values indicate no NT-proBNP measurement during the defined time window
--   - Uses labevents table (laboratory values, not vital signs)
--   - Part of cardiac biomarker panel (see also: Troponin, CK-MB tables)
--
-- Unit of Analysis: ICU stays (icustay_id)
--   - Each ICU stay is analyzed independently
--   - One patient can have multiple hospital admissions
--   - One hospital admission can have multiple ICU stays
--   - We find the "first" measurement for EACH ICU stay
--
-- DECISION: Time Window from "intime - 6 hours" to "outtime" (ICU Stay Bounded)
--   NT-proBNP is a CARDIAC BIOMARKER, typically measured when heart failure is
--   suspected or being monitored. We use a bounded window specific to each ICU stay:
--
--   Rationale:
--   - We want the ICU admission baseline (or immediately preceding value)
--   - Start: intime - 6 hours captures pre-ICU labs (ED, floor) that reflect admission state
--   - End: outtime ensures we only capture labs from THIS ICU stay, preventing contamination
--   - Without outtime bound, joining on subject_id could capture labs from future ICU stays
--   - This provides clean temporal boundaries for each ICU episode
--
--   Implementation:
--   - Join on subject_id (lab approach)
--   - Order by charttime (first chronologically within window)
--   - Bounded to [intime - 6h, outtime] to prevent inter-stay contamination
--
-- Clinical Context:
--   NT-proBNP is used to diagnose and assess heart failure severity:
--   - Normal: < 300 pg/mL (varies by age)
--   - Heart failure cutoffs:
--     * < 300 pg/mL: unlikely heart failure
--     * 300-450 pg/mL: borderline
--     * > 450 pg/mL: likely heart failure
--   - Can be elevated to 35,000+ pg/mL in severe heart failure
-- ===============================================================================

DROP TABLE IF EXISTS icu_first_ntprobnp;

CREATE TABLE icu_first_ntprobnp AS
WITH ntprobnp_measurements AS (
    -- Extract all NT-proBNP measurements with temporal ordering per ICU stay
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
        50963       -- NTproBNP (pg/mL) - N-terminal pro-B-type natriuretic peptide
        -- =====================================================================
    )
    -- =========================================================================
    -- VALUE RANGE FILTERS - Edit these thresholds as needed
    -- =========================================================================
    AND le.valuenum > 0         -- Lower limit: lab values must be positive
    AND le.valuenum <= 70000    -- Upper limit: captures severe heart failure, excludes errors
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

    -- First NT-proBNP measurement (within ICU stay bounded window)
    nm.valuenum AS ntprobnp_first,
    nm.charttime AS ntprobnp_first_charttime,
    nm.itemid AS ntprobnp_first_itemid,

    -- Time from ICU admission to measurement (in minutes)
    -- Can be NEGATIVE (measured before ICU admission, within -6h window)
    -- or POSITIVE (measured during ICU stay)
    DATE_DIFF('second', ie.intime, nm.charttime) / 60.0 AS ntprobnp_first_minutes_from_intime

FROM icustays ie
LEFT JOIN ntprobnp_measurements nm
    ON ie.icustay_id = nm.icustay_id
    AND nm.rn = 1  -- Only the first measurement
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

-- Check ITEMID distribution: Which ITEMIDs are most common?
-- SELECT
--     ntprobnp_first_itemid,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_ntprobnp
-- WHERE ntprobnp_first_itemid IS NOT NULL
-- GROUP BY ntprobnp_first_itemid
-- ORDER BY n_stays DESC;

-- Timing analysis: When are first NT-proBNP measurements typically taken?
-- SELECT
--     CASE
--         WHEN ntprobnp_first_minutes_from_intime < -60 THEN 'More than 1h before admission'
--         WHEN ntprobnp_first_minutes_from_intime < 0 THEN 'Within 1h before admission'
--         WHEN ntprobnp_first_minutes_from_intime <= 60 THEN 'Within 1h after admission'
--         WHEN ntprobnp_first_minutes_from_intime <= 360 THEN 'Within 6h after admission'
--         WHEN ntprobnp_first_minutes_from_intime <= 1440 THEN 'Within 24h after admission'
--         ELSE 'After 24h'
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
