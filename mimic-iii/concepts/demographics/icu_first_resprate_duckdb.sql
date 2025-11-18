-- ===============================================================================
-- MIMIC-III DuckDB: First Respiratory Rate per ICU Stay
-- ===============================================================================
-- This query creates a table with the first respiratory rate measurement for each
-- ICU stay in the MIMIC-III database, optimized for DuckDB.
--
-- The table includes:
--   - First respiratory rate value (breaths per minute)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source/device)
--   - Minutes from ICU admission to measurement
--
-- ITEMID Configuration:
--   To modify which ITEMIDs are considered for respiratory rate, edit the list
--   in the WHERE clause within the resprate_measurements CTE below.
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - No filters by default - includes all ICU stays for maximum flexibility
--   - Tracks ITEMID to identify measurement source
--   - NULL values indicate no respiratory rate measurement during ICU stay
--   - Includes both "Total" (spontaneous + ventilator) and spontaneous-only respiratory rates
--   - ITEMID tracked to distinguish between measurement types
--
-- Unit of Analysis: ICU stays (icustay_id)
-- ===============================================================================

DROP TABLE IF EXISTS icu_first_resprate;

CREATE TABLE icu_first_resprate AS
WITH resprate_measurements AS (
    -- Extract all respiratory rate measurements with temporal ordering per ICU stay
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
        615,        -- Resp Rate (Total) (CareVue)
        618,        -- Respiratory Rate (CareVue)
        220210,     -- Respiratory Rate (MetaVision)
        224690      -- Respiratory Rate (Total) (MetaVision)
        -- Note: "Total" includes both spontaneous and ventilator-assisted breaths
        -- Non-total ITEMIDs (618, 220210) capture spontaneous breaths only
        -- =====================================================================
    )
    -- Value range filter: physiologically plausible respiratory rates (0-70 breaths/min)
    -- Filters out data entry errors and artifacts
    AND ce.valuenum > 0
    AND ce.valuenum < 70
)
SELECT
    ie.icustay_id,
    ie.subject_id,
    ie.hadm_id,
    ie.intime AS icu_intime,

    -- First respiratory rate measurement
    rr.valuenum AS resprate_first,
    rr.charttime AS resprate_first_charttime,
    rr.itemid AS resprate_first_itemid,

    -- Time from ICU admission to measurement (in minutes)
    DATE_DIFF('second', ie.intime, rr.charttime) / 60.0 AS resprate_first_minutes_from_intime

FROM icustays ie
LEFT JOIN resprate_measurements rr
    ON ie.icustay_id = rr.icustay_id
    AND rr.rn = 1  -- Only the first measurement
ORDER BY ie.icustay_id;


-- ===============================================================================
-- DIAGNOSTIC QUERIES
-- ===============================================================================
-- These queries help assess data completeness and quality.
-- Uncomment and run as needed for data validation.

-- Check data completeness: How many ICU stays have respiratory rate measurements?
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     SUM(CASE WHEN resprate_first IS NOT NULL THEN 1 ELSE 0 END) AS stays_with_resprate,
--     ROUND(100.0 * SUM(CASE WHEN resprate_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_resprate
-- FROM icu_first_resprate;

-- Distribution of respiratory rate values
-- SELECT
--     MIN(resprate_first) AS min_rr,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY resprate_first) AS p25_rr,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY resprate_first) AS median_rr,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY resprate_first) AS p75_rr,
--     MAX(resprate_first) AS max_rr
-- FROM icu_first_resprate
-- WHERE resprate_first IS NOT NULL;

-- Check ITEMID distribution: Which ITEMIDs are most common?
-- SELECT
--     resprate_first_itemid,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_resprate
-- WHERE resprate_first_itemid IS NOT NULL
-- GROUP BY resprate_first_itemid
-- ORDER BY n_stays DESC;

-- Timing analysis: When are first RR measurements typically taken?
-- SELECT
--     CASE
--         WHEN resprate_first_minutes_from_intime < 0 THEN 'Before admission'
--         WHEN resprate_first_minutes_from_intime <= 60 THEN 'Within 1 hour'
--         WHEN resprate_first_minutes_from_intime <= 360 THEN 'Within 6 hours'
--         WHEN resprate_first_minutes_from_intime <= 1440 THEN 'Within 24 hours'
--         ELSE 'After 24 hours'
--     END AS timing_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_resprate
-- WHERE resprate_first IS NOT NULL
-- GROUP BY timing_category
-- ORDER BY MIN(resprate_first_minutes_from_intime);

-- Check for potential tachypnea (RR > 20) and bradypnea (RR < 12)
-- SELECT
--     CASE
--         WHEN resprate_first < 12 THEN 'Bradypnea (RR < 12)'
--         WHEN resprate_first <= 20 THEN 'Normal (12-20)'
--         WHEN resprate_first <= 30 THEN 'Mild Tachypnea (20-30)'
--         ELSE 'Severe Tachypnea (RR > 30)'
--     END AS rr_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_resprate
-- WHERE resprate_first IS NOT NULL
-- GROUP BY rr_category
-- ORDER BY MIN(resprate_first);
