-- ===============================================================================
-- MIMIC-III DuckDB: First BUN (Blood Urea Nitrogen) per ICU Stay
-- ===============================================================================
-- This query creates a table with the first BUN (Blood Urea Nitrogen) measurement
-- for each ICU stay in the MIMIC-III database, optimized for DuckDB.
--
-- The table includes:
--   - First BUN value within time window (mg/dL)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source)
--   - Minutes from ICU admission to measurement
--
-- ITEMID Configuration:
--   To modify which ITEMIDs are considered for BUN, edit the list
--   in the WHERE clause within the bun_measurements CTE below.
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - No filters by default - includes all ICU stays for maximum flexibility
--   - Tracks ITEMID to identify measurement source
--   - NULL values indicate no BUN measurement during the defined time window
--   - Uses labevents table (laboratory values, not vital signs)
--   - Part of routine chemistry panel (see also: Creatinine, Potassium tables)
--
-- Unit of Analysis: ICU stays (icustay_id)
--   - Each ICU stay is analyzed independently
--   - One patient can have multiple hospital admissions
--   - One hospital admission can have multiple ICU stays
--   - We find the "first" measurement for EACH ICU stay
--
-- DECISION: Time Window from "intime - 6 hours" to "outtime" (ICU Stay Bounded)
--   Unlike sparse labs (lipid panels) which use "closest within hospital admission"
--   approach, BUN uses a bounded window specific to each ICU stay:
--
--   Rationale:
--   - BUN is a ROUTINE lab, measured frequently in ICU (~792k measurements in MIMIC-III)
--   - We want the ICU admission baseline (or immediately preceding value)
--   - Start: intime - 6 hours captures pre-ICU labs (ED, floor) that reflect admission state
--   - End: outtime ensures we only capture labs from THIS ICU stay, preventing contamination
--   - Without outtime bound, joining on subject_id could capture labs from future ICU stays
--   - This provides clean temporal boundaries for each ICU episode
--
--   Implementation:
--   - Join on subject_id (not hadm_id, as BUN can be checked across admissions)
--   - Order by charttime (first chronologically within window)
--   - Bounded to [intime - 6h, outtime] to prevent inter-stay contamination
--   - Time offset exposed in output (minutes from ICU admission)
--
-- CLINICAL CONTEXT:
--   BUN (Blood Urea Nitrogen) measures the amount of nitrogen in blood that comes
--   from urea, a waste product of protein metabolism. BUN is primarily used to
--   assess kidney function, though it can be affected by other factors.
--
--   Part of standard chemistry panel and ROUTINELY measured in ICU.
--
--   Typical use cases in ICU:
--   - Kidney function assessment (with creatinine, forms BUN/Cr ratio)
--   - Acute kidney injury (AKI) detection and monitoring
--   - Fluid status assessment (elevated with dehydration/prerenal azotemia)
--   - Severity scoring (SAPS II, SOFA scores)
--   - GI bleeding assessment (elevated BUN with normal Cr suggests GI bleed)
--   - Nutritional status (low BUN may indicate malnutrition)
--
--   Note: BUN can be affected by:
--   - Kidney function (primary determinant)
--   - Dehydration/volume depletion (prerenal azotemia)
--   - High protein diet or catabolism
--   - GI bleeding (blood proteins are digested)
--   - Liver disease (decreased urea production)
--   - Certain medications (steroids increase, some antibiotics affect)
--
--   BUN/Creatinine Ratio Clinical Significance:
--   - Normal ratio: 10:1 to 20:1
--   - Ratio >20:1 suggests prerenal azotemia (dehydration, decreased renal perfusion)
--   - Ratio <10:1 may indicate liver disease, low protein diet, or intrinsic renal disease
--
--   Reference Ranges:
--   - Normal: 7-20 mg/dL (varies slightly by lab)
--   - Mild elevation: 21-40 mg/dL
--   - Moderate elevation: 41-80 mg/dL
--   - Severe elevation: >80 mg/dL (associated with uremic symptoms)
--
--   SAPS II Scoring (Severity):
--   - BUN <28 mg/dL: 0 points
--   - BUN 28-84 mg/dL: 6 points
--   - BUN ≥84 mg/dL: 10 points
-- ===============================================================================

DROP TABLE IF EXISTS icu_first_bun;

CREATE TABLE icu_first_bun AS
WITH bun_measurements AS (
    -- Extract first BUN measurement within ICU stay time window
    SELECT
        ie.icustay_id,
        le.charttime,
        le.itemid,
        le.valuenum,
        -- Calculate time difference from ICU admission
        DATE_DIFF('second', ie.intime, le.charttime) AS seconds_from_intime,
        ROW_NUMBER() OVER (
            PARTITION BY ie.icustay_id    -- Each ICU stay analyzed independently
            ORDER BY le.charttime         -- First chronologically within bounded window
        ) AS rn
    FROM icustays ie
    INNER JOIN labevents le
        ON ie.subject_id = le.subject_id  -- Join at PATIENT level (routine lab approach)
    WHERE le.itemid IN (
        -- =====================================================================
        -- EDIT THIS LIST to configure which ITEMIDs to include
        -- =====================================================================
        51006       -- Blood Urea Nitrogen (BUN) - ~792k measurements in MIMIC-III
        -- =====================================================================
    )
    -- =========================================================================
    -- VALUE RANGE FILTERS - Edit these thresholds as needed
    -- =========================================================================
    AND le.valuenum > 0         -- Lower limit: lab values must be positive
    AND le.valuenum <= 300      -- Upper limit: filters extreme outliers (mg/dL)
    -- Normal range: 7-20 mg/dL
    -- Severe elevation: >80 mg/dL (uremic symptoms)
    -- Values >300 mg/dL are extremely rare (severe uremia/renal failure)
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

    -- First BUN measurement (within ICU stay bounded window)
    bm.valuenum AS bun_first,
    bm.charttime AS bun_first_charttime,
    bm.itemid AS bun_first_itemid,

    -- Time from ICU admission to measurement (in minutes)
    -- Can be NEGATIVE (measured before ICU admission, within -6h window)
    -- or POSITIVE (measured during ICU stay)
    bm.seconds_from_intime / 60.0 AS bun_first_minutes_from_intime

FROM icustays ie
LEFT JOIN bun_measurements bm
    ON ie.icustay_id = bm.icustay_id
    AND bm.rn = 1  -- Only the first measurement (chronologically)
ORDER BY ie.icustay_id;


-- ===============================================================================
-- DIAGNOSTIC QUERIES
-- ===============================================================================
-- These queries help assess data completeness and quality.
-- Uncomment and run as needed for data validation.

-- Check data completeness: How many ICU stays have BUN measurements?
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     SUM(CASE WHEN bun_first IS NOT NULL THEN 1 ELSE 0 END) AS stays_with_bun,
--     ROUND(100.0 * SUM(CASE WHEN bun_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_bun
-- FROM icu_first_bun;

-- Distribution of BUN values
-- SELECT
--     MIN(bun_first) AS min_bun,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY bun_first) AS p25_bun,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY bun_first) AS median_bun,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY bun_first) AS p75_bun,
--     MAX(bun_first) AS max_bun
-- FROM icu_first_bun
-- WHERE bun_first IS NOT NULL;

-- Check ITEMID distribution (should only be 51006 unless ITEMIDs are modified)
-- SELECT
--     bun_first_itemid,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_bun
-- WHERE bun_first_itemid IS NOT NULL
-- GROUP BY bun_first_itemid
-- ORDER BY n_stays DESC;

-- Timing analysis: When are first BUN measurements typically taken relative to ICU admission?
-- SELECT
--     CASE
--         WHEN bun_first_minutes_from_intime < -300 THEN '5-6 hours before ICU'
--         WHEN bun_first_minutes_from_intime < -240 THEN '4-5 hours before ICU'
--         WHEN bun_first_minutes_from_intime < -180 THEN '3-4 hours before ICU'
--         WHEN bun_first_minutes_from_intime < -120 THEN '2-3 hours before ICU'
--         WHEN bun_first_minutes_from_intime < -60 THEN '1-2 hours before ICU'
--         WHEN bun_first_minutes_from_intime < 0 THEN 'Within 1h before ICU'
--         WHEN bun_first_minutes_from_intime <= 60 THEN 'Within 1h after ICU'
--         WHEN bun_first_minutes_from_intime <= 360 THEN '1-6 hours after ICU'
--         WHEN bun_first_minutes_from_intime <= 1440 THEN '6-24 hours after ICU'
--         ELSE 'More than 24h after ICU'
--     END AS timing_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_bun
-- WHERE bun_first IS NOT NULL
-- GROUP BY timing_category
-- ORDER BY MIN(bun_first_minutes_from_intime);

-- Check BUN level categories (kidney function assessment)
-- SELECT
--     CASE
--         WHEN bun_first <= 20 THEN 'Normal (≤ 20)'
--         WHEN bun_first <= 40 THEN 'Mild Elevation (21-40)'
--         WHEN bun_first <= 80 THEN 'Moderate Elevation (41-80)'
--         ELSE 'Severe Elevation (> 80) - Uremic Risk'
--     END AS bun_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_bun
-- WHERE bun_first IS NOT NULL
-- GROUP BY bun_category
-- ORDER BY MIN(bun_first);

-- Check BUN by SAPS II scoring categories (severity assessment)
-- SELECT
--     CASE
--         WHEN bun_first < 28 THEN 'SAPS II: 0 points (< 28)'
--         WHEN bun_first < 84 THEN 'SAPS II: 6 points (28-83)'
--         ELSE 'SAPS II: 10 points (≥ 84)'
--     END AS saps_ii_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_bun
-- WHERE bun_first IS NOT NULL
-- GROUP BY saps_ii_category
-- ORDER BY MIN(bun_first);

-- Analyze measurements captured before vs during ICU stay
-- SELECT
--     CASE
--         WHEN bun_first_minutes_from_intime < 0 THEN 'Before ICU admission (within -6h window)'
--         ELSE 'During ICU stay'
--     END AS measurement_timing,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
--     ROUND(AVG(bun_first), 2) AS avg_bun,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY bun_first), 2) AS median_bun
-- FROM icu_first_bun
-- WHERE bun_first IS NOT NULL
-- GROUP BY measurement_timing
-- ORDER BY MIN(bun_first_minutes_from_intime);
