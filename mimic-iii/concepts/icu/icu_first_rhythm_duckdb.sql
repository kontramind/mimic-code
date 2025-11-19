-- ===============================================================================
-- MIMIC-III DuckDB: First Heart Rhythm per ICU Stay
-- ===============================================================================
-- This query creates a table with the first heart rhythm measurement for each
-- ICU stay in the MIMIC-III database, optimized for DuckDB.
--
-- The table includes:
--   - First heart rhythm value (categorical/text)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source/device)
--   - Minutes from ICU admission to measurement
--
-- ITEMID Configuration:
--   To modify which ITEMIDs are considered for heart rhythm, edit the list
--   in the WHERE clause within the rhythm_measurements CTE below.
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - No filters by default - includes all ICU stays for maximum flexibility
--   - Tracks ITEMID to identify measurement source
--   - NULL values indicate no heart rhythm measurement during ICU stay
--   - Rhythm is typically a categorical value (e.g., "Sinus Rhythm", "Atrial Fibrillation")
--
-- Unit of Analysis: ICU stays (icustay_id)
-- ===============================================================================

DROP TABLE IF EXISTS icu_first_rhythm;

CREATE TABLE icu_first_rhythm AS
WITH rhythm_measurements AS (
    -- Extract all heart rhythm measurements with temporal ordering per ICU stay
    SELECT
        ie.icustay_id,
        ce.charttime,
        ce.itemid,
        ce.value,  -- Rhythm is typically stored as text/categorical value
        ROW_NUMBER() OVER (PARTITION BY ie.icustay_id ORDER BY ce.charttime) AS rn
    FROM icustays ie
    INNER JOIN chartevents ce
        ON ie.icustay_id = ce.icustay_id
    WHERE ce.itemid IN (
        -- =====================================================================
        -- EDIT THIS LIST to configure which ITEMIDs to include
        -- =====================================================================
        212,        -- Heart Rhythm (CareVue)
        3354,       -- Heart Rhythm
        5119,       -- Heart Rhythm
        220048      -- Heart Rhythm (MetaVision)
        -- =====================================================================
    )
    -- Note: Heart rhythm is typically a categorical value, no numeric filtering
    -- =========================================================================
    -- TIME WINDOW - ROUTINE vital sign pattern
    -- =========================================================================
    AND ce.charttime >= ie.intime - INTERVAL '6' HOUR  -- Capture pre-ICU measurements
    AND ce.charttime <= ie.outtime                     -- Bound to ICU stay
    -- =========================================================================
)
SELECT
    ie.icustay_id,
    ie.subject_id,
    ie.hadm_id,
    ie.intime AS icu_intime,

    -- First heart rhythm measurement
    rhythm.value AS rhythm_first,
    rhythm.charttime AS rhythm_first_charttime,
    rhythm.itemid AS rhythm_first_itemid,

    -- Time from ICU admission to measurement (in minutes)
    DATE_DIFF('second', ie.intime, rhythm.charttime) / 60.0 AS rhythm_first_minutes_from_intime

FROM icustays ie
LEFT JOIN rhythm_measurements rhythm
    ON ie.icustay_id = rhythm.icustay_id
    AND rhythm.rn = 1  -- Only the first measurement
ORDER BY ie.icustay_id;


-- ===============================================================================
-- DIAGNOSTIC QUERIES
-- ===============================================================================
-- These queries help assess data completeness and quality.
-- Uncomment and run as needed for data validation.

-- Check data completeness: How many ICU stays have rhythm measurements?
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     SUM(CASE WHEN rhythm_first IS NOT NULL THEN 1 ELSE 0 END) AS stays_with_rhythm,
--     ROUND(100.0 * SUM(CASE WHEN rhythm_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_rhythm
-- FROM icu_first_rhythm;

-- Distribution of rhythm values: What are the most common rhythms?
-- SELECT
--     rhythm_first,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_rhythm
-- WHERE rhythm_first IS NOT NULL
-- GROUP BY rhythm_first
-- ORDER BY n_stays DESC
-- LIMIT 20;

-- Check ITEMID distribution: Which ITEMIDs are most common?
-- SELECT
--     rhythm_first_itemid,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_rhythm
-- WHERE rhythm_first_itemid IS NOT NULL
-- GROUP BY rhythm_first_itemid
-- ORDER BY n_stays DESC;

-- Timing analysis: When are first rhythm measurements typically taken?
-- SELECT
--     CASE
--         WHEN rhythm_first_minutes_from_intime < 0 THEN 'Before admission'
--         WHEN rhythm_first_minutes_from_intime <= 60 THEN 'Within 1 hour'
--         WHEN rhythm_first_minutes_from_intime <= 360 THEN 'Within 6 hours'
--         WHEN rhythm_first_minutes_from_intime <= 1440 THEN 'Within 24 hours'
--         ELSE 'After 24 hours'
--     END AS timing_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_rhythm
-- WHERE rhythm_first IS NOT NULL
-- GROUP BY timing_category
-- ORDER BY MIN(rhythm_first_minutes_from_intime);

-- Cross-tabulation: Rhythm by ITEMID (to see if different ITEMIDs capture different rhythms)
-- SELECT
--     rhythm_first_itemid,
--     rhythm_first,
--     COUNT(*) AS n_stays
-- FROM icu_first_rhythm
-- WHERE rhythm_first IS NOT NULL
-- GROUP BY rhythm_first_itemid, rhythm_first
-- ORDER BY rhythm_first_itemid, n_stays DESC;

-- Check for abnormal rhythms (Atrial Fibrillation, Ventricular Tachycardia, etc.)
-- SELECT
--     CASE
--         WHEN LOWER(rhythm_first) LIKE '%sinus%' THEN 'Sinus Rhythm'
--         WHEN LOWER(rhythm_first) LIKE '%atrial fib%' OR LOWER(rhythm_first) LIKE '%a fib%' THEN 'Atrial Fibrillation'
--         WHEN LOWER(rhythm_first) LIKE '%atrial flutter%' THEN 'Atrial Flutter'
--         WHEN LOWER(rhythm_first) LIKE '%paced%' THEN 'Paced'
--         WHEN LOWER(rhythm_first) LIKE '%v tach%' OR LOWER(rhythm_first) LIKE '%ventricular tach%' THEN 'Ventricular Tachycardia'
--         WHEN LOWER(rhythm_first) LIKE '%junctional%' THEN 'Junctional'
--         ELSE 'Other'
--     END AS rhythm_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_rhythm
-- WHERE rhythm_first IS NOT NULL
-- GROUP BY rhythm_category
-- ORDER BY n_stays DESC;
