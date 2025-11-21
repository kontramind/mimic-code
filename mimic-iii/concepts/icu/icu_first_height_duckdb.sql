-- ===============================================================================
-- MIMIC-III DuckDB: First Height per ICU Stay
-- ===============================================================================
-- This query creates a table with the first height measurement for each
-- ICU stay in the MIMIC-III database, optimized for DuckDB.
--
-- The table includes:
--   - First height value (cm)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source)
--   - Minutes from ICU admission to measurement
--
-- ITEMID Configuration:
--   Height in inches (converted to cm):
--     920, 1394, 4187, 3486 (CareVue)
--     226707 (MetaVision)
--   Height in cm:
--     3485, 4188 (CareVue)
--     226730 (MetaVision)
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - Converts all heights to centimeters
--   - Filters outliers: adults 120-230 cm (reasonable adult range)
--   - Height is relatively static - unlikely to change during stay
--   - NULL values indicate no height measurement during ICU stay
--
-- Unit of Analysis: ICU stays (icustay_id)
-- ===============================================================================

DROP TABLE IF EXISTS icu_first_height;

CREATE TABLE icu_first_height AS
WITH height_measurements AS (
    -- Extract all height measurements with unit conversion and temporal ordering
    SELECT
        ie.icustay_id,
        ce.charttime,
        ce.itemid,
        -- Convert inches to centimeters where needed
        CASE
            WHEN ce.itemid IN (920, 1394, 4187, 3486, 226707)  -- inches
            THEN ce.valuenum * 2.54
            ELSE ce.valuenum  -- already in cm
        END AS height_cm,
        ROW_NUMBER() OVER (PARTITION BY ie.icustay_id ORDER BY ce.charttime) AS rn
    FROM icustays ie
    INNER JOIN chartevents ce
        ON ie.icustay_id = ce.icustay_id
    WHERE ce.itemid IN (
        -- =====================================================================
        -- Height ITEMIDs
        -- =====================================================================
        -- CareVue (inches)
        920,        -- Height
        1394,       -- Height Inches
        4187,       -- Height Inches
        3486,       -- Height Inches
        -- CareVue (cm)
        3485,       -- Height cm
        4188,       -- Height cm
        -- MetaVision (inches)
        226707,     -- Height (inches)
        -- MetaVision (cm)
        226730      -- Height (cm)
        -- =====================================================================
    )
    AND ce.valuenum IS NOT NULL
    AND ce.valuenum > 0
    -- Exclude rows marked as error
    AND (ce.error IS NULL OR ce.error = 0)
    -- =========================================================================
    -- TIME WINDOW - Height can be measured anytime during stay
    -- =========================================================================
    AND ce.charttime >= ie.intime - INTERVAL '1' DAY   -- Some fuzziness for admit
    AND ce.charttime <= ie.outtime
    -- =========================================================================
),
height_filtered AS (
    -- Apply physiologically plausible range filter AFTER unit conversion
    SELECT *
    FROM height_measurements
    WHERE height_cm >= 120   -- Minimum adult height
    AND height_cm <= 230     -- Maximum adult height
)
SELECT
    ie.icustay_id,
    ie.subject_id,
    ie.hadm_id,
    ie.intime AS icu_intime,

    -- First height measurement (in cm)
    ROUND(hf.height_cm, 1) AS height_first,
    hf.charttime AS height_first_charttime,
    hf.itemid AS height_first_itemid,

    -- Time from ICU admission to measurement (in minutes)
    DATE_DIFF('second', ie.intime, hf.charttime) / 60.0 AS height_first_minutes_from_intime

FROM icustays ie
LEFT JOIN height_filtered hf
    ON ie.icustay_id = hf.icustay_id
    AND hf.rn = 1  -- Only the first measurement
ORDER BY ie.icustay_id;


-- ===============================================================================
-- DIAGNOSTIC QUERIES
-- ===============================================================================

-- Check data completeness: How many ICU stays have height measurements?
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     SUM(CASE WHEN height_first IS NOT NULL THEN 1 ELSE 0 END) AS stays_with_height,
--     ROUND(100.0 * SUM(CASE WHEN height_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_height
-- FROM icu_first_height;

-- Distribution of height values
-- SELECT
--     MIN(height_first) AS min_height,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY height_first) AS p25_height,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY height_first) AS median_height,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY height_first) AS p75_height,
--     MAX(height_first) AS max_height
-- FROM icu_first_height
-- WHERE height_first IS NOT NULL;

-- Check ITEMID distribution: Which ITEMIDs are most common?
-- SELECT
--     height_first_itemid,
--     CASE
--         WHEN height_first_itemid IN (920, 1394, 4187, 3486, 226707) THEN 'inches (converted)'
--         ELSE 'cm (native)'
--     END AS unit_source,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_height
-- WHERE height_first_itemid IS NOT NULL
-- GROUP BY height_first_itemid
-- ORDER BY n_stays DESC;

-- Height distribution by category
-- SELECT
--     CASE
--         WHEN height_first < 150 THEN '<150 cm'
--         WHEN height_first < 160 THEN '150-159 cm'
--         WHEN height_first < 170 THEN '160-169 cm'
--         WHEN height_first < 180 THEN '170-179 cm'
--         WHEN height_first < 190 THEN '180-189 cm'
--         ELSE '>=190 cm'
--     END AS height_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_height
-- WHERE height_first IS NOT NULL
-- GROUP BY height_category
-- ORDER BY MIN(height_first);
