-- ===============================================================================
-- MIMIC-III DuckDB: First Weight per ICU Stay
-- ===============================================================================
-- This query creates a table with the first weight measurement for each
-- ICU stay in the MIMIC-III database, optimized for DuckDB.
--
-- The table includes:
--   - First weight value (kg)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source: admit vs daily weight)
--   - Minutes from ICU admission to measurement
--
-- ITEMID Configuration:
--   Admission Weight:
--     762 (CareVue) - Admit Wt
--     226512 (MetaVision) - Admission Weight (kg)
--   Daily Weight:
--     763 (CareVue) - Daily Weight
--     224639 (MetaVision) - Daily Weight
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - All weights are in kilograms (native unit in MIMIC-III)
--   - Filters outliers: 30-300 kg (reasonable adult range)
--   - Prioritizes admission weight over daily weight via temporal ordering
--   - Weight may change during ICU stay (fluid balance) - this captures FIRST value
--   - NULL values indicate no weight measurement during ICU stay
--
-- Clinical Note:
--   Weight is important for drug dosing, severity scores, and nutritional assessment.
--   First weight (admission weight) is typically more reliable than daily weights
--   which can be affected by fluid status.
--
-- Unit of Analysis: ICU stays (icustay_id)
-- ===============================================================================

DROP TABLE IF EXISTS icu_first_weight;

CREATE TABLE icu_first_weight AS
WITH weight_measurements AS (
    -- Extract all weight measurements with temporal ordering per ICU stay
    SELECT
        ie.icustay_id,
        ce.charttime,
        ce.itemid,
        ce.valuenum AS weight_kg,
        -- Prioritize admission weight over daily weight
        CASE
            WHEN ce.itemid IN (762, 226512) THEN 1  -- Admission weight (priority)
            ELSE 2  -- Daily weight
        END AS weight_priority,
        ROW_NUMBER() OVER (
            PARTITION BY ie.icustay_id
            ORDER BY
                CASE WHEN ce.itemid IN (762, 226512) THEN 1 ELSE 2 END,  -- Admission weight first
                ce.charttime  -- Then by time
        ) AS rn
    FROM icustays ie
    INNER JOIN chartevents ce
        ON ie.icustay_id = ce.icustay_id
    WHERE ce.itemid IN (
        -- =====================================================================
        -- Weight ITEMIDs (all in kg)
        -- =====================================================================
        -- Admission Weight (preferred - more accurate)
        762,        -- Admit Wt (CareVue)
        226512,     -- Admission Weight (kg) (MetaVision)
        -- Daily Weight (backup)
        763,        -- Daily Weight (CareVue)
        224639      -- Daily Weight (MetaVision)
        -- =====================================================================
    )
    AND ce.valuenum IS NOT NULL
    AND ce.valuenum > 0
    -- Exclude rows marked as error
    AND (ce.error IS NULL OR ce.error = 0)
    -- =========================================================================
    -- TIME WINDOW - First day of ICU stay
    -- =========================================================================
    AND ce.charttime >= ie.intime - INTERVAL '1' DAY   -- Some fuzziness for admit
    AND ce.charttime <= ie.intime + INTERVAL '1' DAY   -- First day only
    -- =========================================================================
    -- Physiologically plausible weight range (adults)
    AND ce.valuenum >= 30    -- Minimum adult weight (kg)
    AND ce.valuenum <= 300   -- Maximum adult weight (kg)
)
SELECT
    ie.icustay_id,
    ie.subject_id,
    ie.hadm_id,
    ie.intime AS icu_intime,

    -- First weight measurement (in kg)
    ROUND(wt.weight_kg, 2) AS weight_first,
    wt.charttime AS weight_first_charttime,
    wt.itemid AS weight_first_itemid,

    -- Weight source type
    CASE
        WHEN wt.itemid IN (762, 226512) THEN 'admission'
        WHEN wt.itemid IN (763, 224639) THEN 'daily'
        ELSE NULL
    END AS weight_source,

    -- Time from ICU admission to measurement (in minutes)
    DATE_DIFF('second', ie.intime, wt.charttime) / 60.0 AS weight_first_minutes_from_intime

FROM icustays ie
LEFT JOIN weight_measurements wt
    ON ie.icustay_id = wt.icustay_id
    AND wt.rn = 1  -- Only the first measurement (admission weight prioritized)
ORDER BY ie.icustay_id;


-- ===============================================================================
-- DIAGNOSTIC QUERIES
-- ===============================================================================

-- Check data completeness: How many ICU stays have weight measurements?
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     SUM(CASE WHEN weight_first IS NOT NULL THEN 1 ELSE 0 END) AS stays_with_weight,
--     ROUND(100.0 * SUM(CASE WHEN weight_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_weight
-- FROM icu_first_weight;

-- Distribution of weight values
-- SELECT
--     MIN(weight_first) AS min_weight,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY weight_first) AS p25_weight,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY weight_first) AS median_weight,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY weight_first) AS p75_weight,
--     MAX(weight_first) AS max_weight
-- FROM icu_first_weight
-- WHERE weight_first IS NOT NULL;

-- Check weight source distribution (admission vs daily weight)
-- SELECT
--     weight_source,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
--     ROUND(AVG(weight_first), 2) AS avg_weight
-- FROM icu_first_weight
-- WHERE weight_first IS NOT NULL
-- GROUP BY weight_source
-- ORDER BY n_stays DESC;

-- Weight distribution by BMI-relevant categories (assuming avg height ~170cm)
-- SELECT
--     CASE
--         WHEN weight_first < 50 THEN '<50 kg (underweight)'
--         WHEN weight_first < 70 THEN '50-69 kg'
--         WHEN weight_first < 90 THEN '70-89 kg'
--         WHEN weight_first < 110 THEN '90-109 kg'
--         WHEN weight_first < 130 THEN '110-129 kg'
--         ELSE '>=130 kg (obese)'
--     END AS weight_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_weight
-- WHERE weight_first IS NOT NULL
-- GROUP BY weight_category
-- ORDER BY MIN(weight_first);

-- Check ITEMID distribution
-- SELECT
--     weight_first_itemid,
--     weight_source,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_weight
-- WHERE weight_first_itemid IS NOT NULL
-- GROUP BY weight_first_itemid, weight_source
-- ORDER BY n_stays DESC;
