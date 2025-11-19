-- ===============================================================================
-- MIMIC-III DuckDB: First Potassium per ICU Stay
-- ===============================================================================
-- This query creates a table with the first potassium measurement for each
-- ICU stay in the MIMIC-III database, optimized for DuckDB.
--
-- The table includes:
--   - First potassium value (mEq/L)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source/method)
--   - Minutes from ICU admission to measurement
--
-- ITEMID Configuration:
--   To modify which ITEMIDs are considered for potassium, edit the list
--   in the WHERE clause within the potassium_measurements CTE below.
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - No filters by default - includes all ICU stays for maximum flexibility
--   - Tracks ITEMID to identify measurement source (chemistry vs blood gas)
--   - NULL values indicate no potassium measurement during the defined time window
--   - Uses labevents table (laboratory values, not vital signs)
--   - Includes BOTH chemistry panel and blood gas potassium measurements
--   - Part of routine chemistry panel (see also: Creatinine, BUN tables)
--
-- Unit of Analysis: ICU stays (icustay_id)
--   - Each ICU stay is analyzed independently
--   - One patient can have multiple hospital admissions
--   - One hospital admission can have multiple ICU stays
--   - We find the "first" measurement relative to EACH ICU stay's admission time
--
-- DECISION: Time Window from "intime - 6 hours" to "outtime" (ICU Stay Bounded)
--   Potassium is a ROUTINE lab, measured frequently in ICU (~1M measurements).
--   We use a bounded window specific to each ICU stay:
--
--   Rationale:
--   - We want the ICU admission baseline (or immediately preceding value)
--   - Start: intime - 6 hours captures pre-ICU labs (ED, floor) that reflect admission state
--   - End: outtime ensures we only capture labs from THIS ICU stay, preventing contamination
--   - Without outtime bound, joining on subject_id could capture labs from future ICU stays
--   - This provides clean temporal boundaries for each ICU episode
--
--   Implementation:
--   - Join on subject_id (routine lab approach)
--   - Order by charttime (first chronologically within window)
--   - Bounded to [intime - 6h, outtime] to prevent inter-stay contamination
--
-- DECISION: Multiple ITEMIDs for Potassium
--   We include BOTH potassium measurement sources:
--   - ITEMID 50971: Standard chemistry panel potassium (~845k measurements)
--   - ITEMID 50822: Whole blood potassium from blood gas (~193k measurements)
--
--   Rationale:
--   - Maximizes data completeness (~1M total measurements vs ~800k if using only chemistry)
--   - Both are clinically valid and used interchangeably
--   - Blood gas K+ often available first in critical situations (faster turnaround)
--   - The ITEMID is tracked in output so measurement source is known
--   - Methodological difference (whole blood vs serum) is usually clinically insignificant
--
-- DECISION: "Earliest" means relative to ICU stay admission
--   When we say "first" or "earliest" potassium, we mean:
--   - The measurement with the earliest charttime within the time window
--   - Relative to that specific ICU stay's admission time (intime)
--   - NOT earliest across the entire hospital admission
--   - NOT earliest across all ICU stays for that patient
--
--   Example: If a patient has 2 ICU stays during one hospital admission,
--   each ICU stay gets its own independent "first" potassium measurement
--   based on that ICU stay's admission time.
--
--   The PARTITION BY icustay_id ensures each ICU stay is analyzed separately.
-- ===============================================================================

DROP TABLE IF EXISTS icu_first_potassium;

CREATE TABLE icu_first_potassium AS
WITH potassium_measurements AS (
    -- Extract all potassium measurements with temporal ordering per ICU stay
    SELECT
        ie.icustay_id,
        le.charttime,
        le.itemid,
        le.valuenum,
        ROW_NUMBER() OVER (
            PARTITION BY ie.icustay_id    -- Each ICU stay analyzed independently
            ORDER BY le.charttime          -- Earliest by time (regardless of ITEMID)
        ) AS rn
    FROM icustays ie
    INNER JOIN labevents le
        ON ie.subject_id = le.subject_id
    WHERE le.itemid IN (
        -- =====================================================================
        -- EDIT THIS LIST to configure which ITEMIDs to include
        -- =====================================================================
        50971,      -- Potassium (Chemistry panel) - most common
        50822       -- Potassium, Whole Blood (Blood gas) - faster turnaround
        -- Both ITEMIDs are combined: whichever comes first chronologically
        -- is selected, regardless of measurement method
        -- =====================================================================
    )
    -- =========================================================================
    -- VALUE RANGE FILTERS - Edit these thresholds as needed
    -- =========================================================================
    AND le.valuenum > 0         -- Lower limit: lab values must be positive
    AND le.valuenum <= 30       -- Upper limit: filters out data entry errors (mEq/L)
    -- Normal range: 3.5-5.0 mEq/L
    -- Critical low: < 3.0 mEq/L (hypokalemia - cardiac arrhythmia risk)
    -- Critical high: > 5.0 mEq/L (hyperkalemia - cardiac arrest risk)
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

    -- First potassium measurement (within ICU stay bounded window)
    pm.valuenum AS potassium_first,
    pm.charttime AS potassium_first_charttime,
    pm.itemid AS potassium_first_itemid,

    -- Time from ICU admission to measurement (in minutes)
    -- Can be NEGATIVE (measured before ICU admission, within -6h window)
    -- or POSITIVE (measured during ICU stay, earliest by charttime)
    DATE_DIFF('second', ie.intime, pm.charttime) / 60.0 AS potassium_first_minutes_from_intime

FROM icustays ie
LEFT JOIN potassium_measurements pm
    ON ie.icustay_id = pm.icustay_id
    AND pm.rn = 1  -- Only the first measurement (earliest by charttime)
ORDER BY ie.icustay_id;


-- ===============================================================================
-- DIAGNOSTIC QUERIES
-- ===============================================================================
-- These queries help assess data completeness and quality.
-- Uncomment and run as needed for data validation.

-- Check data completeness: How many ICU stays have potassium measurements?
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     SUM(CASE WHEN potassium_first IS NOT NULL THEN 1 ELSE 0 END) AS stays_with_potassium,
--     ROUND(100.0 * SUM(CASE WHEN potassium_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_potassium
-- FROM icu_first_potassium;

-- Distribution of potassium values
-- SELECT
--     MIN(potassium_first) AS min_k,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY potassium_first) AS p25_k,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY potassium_first) AS median_k,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY potassium_first) AS p75_k,
--     MAX(potassium_first) AS max_k
-- FROM icu_first_potassium
-- WHERE potassium_first IS NOT NULL;

-- Check ITEMID distribution: Chemistry vs Blood Gas sources
-- SELECT
--     potassium_first_itemid,
--     CASE
--         WHEN potassium_first_itemid = 50971 THEN 'Chemistry Panel'
--         WHEN potassium_first_itemid = 50822 THEN 'Blood Gas (Whole Blood)'
--         ELSE 'Unknown'
--     END AS measurement_source,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_potassium
-- WHERE potassium_first_itemid IS NOT NULL
-- GROUP BY potassium_first_itemid
-- ORDER BY n_stays DESC;

-- Timing analysis: When are first potassium measurements typically taken?
-- SELECT
--     CASE
--         WHEN potassium_first_minutes_from_intime < -60 THEN 'More than 1h before admission'
--         WHEN potassium_first_minutes_from_intime < 0 THEN 'Within 1h before admission'
--         WHEN potassium_first_minutes_from_intime <= 60 THEN 'Within 1h after admission'
--         WHEN potassium_first_minutes_from_intime <= 360 THEN 'Within 6h after admission'
--         WHEN potassium_first_minutes_from_intime <= 1440 THEN 'Within 24h after admission'
--         ELSE 'After 24h'
--     END AS timing_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_potassium
-- WHERE potassium_first IS NOT NULL
-- GROUP BY timing_category
-- ORDER BY MIN(potassium_first_minutes_from_intime);

-- Check for hypokalemia and hyperkalemia (used in SAPS II severity scoring)
-- Normal range: 3.5-5.0 mEq/L (some sources use 3.0-5.0)
-- SELECT
--     CASE
--         WHEN potassium_first < 2.5 THEN 'Severe Hypokalemia (< 2.5)'
--         WHEN potassium_first < 3.0 THEN 'Moderate Hypokalemia (2.5-3.0)'
--         WHEN potassium_first < 3.5 THEN 'Mild Hypokalemia (3.0-3.5)'
--         WHEN potassium_first <= 5.0 THEN 'Normal (3.5-5.0)'
--         WHEN potassium_first <= 5.5 THEN 'Mild Hyperkalemia (5.0-5.5)'
--         WHEN potassium_first <= 6.0 THEN 'Moderate Hyperkalemia (5.5-6.0)'
--         ELSE 'Severe Hyperkalemia (> 6.0)'
--     END AS potassium_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_potassium
-- WHERE potassium_first IS NOT NULL
-- GROUP BY potassium_category
-- ORDER BY MIN(potassium_first);
