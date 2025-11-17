-- ===============================================================================
-- MIMIC-III DuckDB: First Heart Rate per ICU Stay
-- ===============================================================================
-- This query creates a table with the first heart rate measurement for each
-- ICU stay in the MIMIC-III database, optimized for DuckDB.
--
-- The table includes:
--   - First heart rate value (bpm)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source/device)
--   - Minutes from ICU admission to measurement
--
-- ITEMID Configuration:
--   To modify which ITEMIDs are considered for heart rate, edit the list
--   in the WHERE clause within the hr_measurements CTE below.
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - No filters by default - includes all ICU stays for maximum flexibility
--   - Tracks ITEMID to identify measurement source
--   - NULL values indicate no heart rate measurement during ICU stay
--
-- Unit of Analysis: ICU stays (icustay_id)
-- ===============================================================================

DROP TABLE IF EXISTS icu_first_hr;

CREATE TABLE icu_first_hr AS
WITH hr_measurements AS (
    -- Extract all heart rate measurements with temporal ordering per ICU stay
    SELECT
        ie.icustay_id,
        ce.charttime,
        ce.itemid,
        ce.valuenum,
        ROW_NUMBER() OVER (PARTITION BY ie.icustay_id ORDER BY ce.charttime) AS rn
    FROM icustays ie
    INNER JOIN chartevents ce
        ON ie.icustay_id = ce.icustay_id
    WHERE ce.itemid IN (
        -- =====================================================================
        -- EDIT THIS LIST to configure which ITEMIDs to include
        -- =====================================================================
        222,        -- Heart Rate (check D_ITEMS for label)
        220045      -- Heart Rate (MetaVision)
        -- Note: Common alternative ITEMID is 211 (CareVue Heart Rate)
        -- =====================================================================
    )
    -- Value range filter: physiologically plausible heart rates (0-300 bpm)
    -- Filters out data entry errors and artifacts
    AND ce.valuenum > 0
    AND ce.valuenum < 300
)
SELECT
    ie.icustay_id,
    ie.subject_id,
    ie.hadm_id,
    ie.intime AS icu_intime,

    -- First heart rate measurement
    hr.valuenum AS hr_first,
    hr.charttime AS hr_first_charttime,
    hr.itemid AS hr_first_itemid,

    -- Time from ICU admission to measurement (in minutes)
    DATE_DIFF('second', ie.intime, hr.charttime) / 60.0 AS hr_first_minutes_from_intime

FROM icustays ie
LEFT JOIN hr_measurements hr
    ON ie.icustay_id = hr.icustay_id
    AND hr.rn = 1  -- Only the first measurement
ORDER BY ie.icustay_id;


-- ===============================================================================
-- DIAGNOSTIC QUERIES
-- ===============================================================================
-- These queries help assess data completeness and quality.
-- Uncomment and run as needed for data validation.

-- Check data completeness: How many ICU stays have heart rate measurements?
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     SUM(CASE WHEN hr_first IS NOT NULL THEN 1 ELSE 0 END) AS stays_with_hr,
--     ROUND(100.0 * SUM(CASE WHEN hr_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_hr
-- FROM icu_first_hr;

-- Distribution of heart rate values
-- SELECT
--     MIN(hr_first) AS min_hr,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY hr_first) AS p25_hr,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY hr_first) AS median_hr,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY hr_first) AS p75_hr,
--     MAX(hr_first) AS max_hr
-- FROM icu_first_hr
-- WHERE hr_first IS NOT NULL;

-- Check ITEMID distribution: Which ITEMIDs are most common?
-- SELECT
--     hr_first_itemid,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_hr
-- WHERE hr_first_itemid IS NOT NULL
-- GROUP BY hr_first_itemid
-- ORDER BY n_stays DESC;

-- Timing analysis: When are first HR measurements typically taken?
-- SELECT
--     CASE
--         WHEN hr_first_minutes_from_intime < 0 THEN 'Before admission'
--         WHEN hr_first_minutes_from_intime <= 60 THEN 'Within 1 hour'
--         WHEN hr_first_minutes_from_intime <= 360 THEN 'Within 6 hours'
--         WHEN hr_first_minutes_from_intime <= 1440 THEN 'Within 24 hours'
--         ELSE 'After 24 hours'
--     END AS timing_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_hr
-- WHERE hr_first IS NOT NULL
-- GROUP BY timing_category
-- ORDER BY MIN(hr_first_minutes_from_intime);

-- Check for potential bradycardia (HR < 60) and tachycardia (HR > 100)
-- SELECT
--     CASE
--         WHEN hr_first < 60 THEN 'Bradycardia (HR < 60)'
--         WHEN hr_first <= 100 THEN 'Normal (60-100)'
--         ELSE 'Tachycardia (HR > 100)'
--     END AS hr_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_hr
-- WHERE hr_first IS NOT NULL
-- GROUP BY hr_category
-- ORDER BY MIN(hr_first);
