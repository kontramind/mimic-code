-- ===============================================================================
-- MIMIC-III DuckDB: Closest LDL Cholesterol per ICU Stay
-- ===============================================================================
-- This query creates a table with the closest LDL cholesterol measurement to
-- ICU admission for each ICU stay in the MIMIC-III database, optimized for DuckDB.
--
-- The table includes:
--   - Closest LDL cholesterol value (mg/dL)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source/method)
--   - Minutes from ICU admission to measurement (can be negative if before)
--
-- ITEMID Configuration:
--   To modify which ITEMIDs are considered for LDL cholesterol, edit the list
--   in the WHERE clause within the ldl_measurements CTE below.
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - No filters by default - includes all ICU stays for maximum flexibility
--   - Tracks ITEMID to identify measurement method (calculated vs measured)
--   - NULL values indicate no LDL cholesterol measurement during hospital stay
--   - Uses labevents table (laboratory values, not vital signs)
--   - Part of lipid panel series (see also: Total Chol, HDL, triglycerides tables)
--   - Includes BOTH calculated and measured LDL values
--
-- Unit of Analysis: ICU stays (icustay_id)
--   - Each ICU stay is analyzed independently
--   - One patient can have multiple hospital admissions
--   - One hospital admission can have multiple ICU stays
--   - We find the "closest" measurement to EACH ICU stay's admission time
--
-- DECISION: Time Window from "intime - 7 days" to "outtime" (ICU Stay Bounded)
--   LDL is a SPARSE LAB, measured infrequently as part of lipid panel.
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
-- DECISION: Multiple ITEMIDs for LDL Cholesterol
--   We include BOTH LDL measurement methods:
--   - ITEMID 50905: LDL Calculated (Friedewald equation) (~233k measurements)
--   - ITEMID 50906: LDL Measured (Direct assay) (~41k measurements)
--
--   Rationale:
--   - Maximizes data completeness (~274k total vs ~233k if using only calculated)
--   - Both methods are clinically valid and used interchangeably
--   - Calculated LDL is standard when triglycerides <400 mg/dL
--   - Measured LDL (direct assay) used when calculated is unreliable
--   - The ITEMID is tracked in output so measurement method is known
--   - Whichever is closest chronologically is selected (calculated or measured)
--
-- CLINICAL CONTEXT:
--   LDL (Low-Density Lipoprotein) cholesterol is known as "bad" cholesterol.
--   High LDL is a major risk factor for atherosclerotic cardiovascular disease.
--
--   Part of standard lipid panel but NOT routinely measured in ICU.
--
--   Typical use cases in ICU:
--   - Post-acute coronary syndrome
--   - Cardiovascular risk stratification
--   - Pre-existing dyslipidemia management
--   - Statin therapy monitoring
--
--   Note: Critical illness can affect LDL levels.
--   Friedewald equation (calculated LDL): LDL = Total Chol - HDL - (Triglycerides/5)
--   This equation is invalid when triglycerides >400 mg/dL or patient is non-fasting.
--
--   Reference Ranges:
--   - Optimal: <100 mg/dL
--   - Near optimal: 100-129 mg/dL
--   - Borderline high: 130-159 mg/dL
--   - High: 160-189 mg/dL
--   - Very high: ≥190 mg/dL
-- ===============================================================================

DROP TABLE IF EXISTS icu_first_ldl;

CREATE TABLE icu_first_ldl AS
WITH ldl_measurements AS (
    -- Extract all LDL cholesterol measurements, ordered by distance from ICU admission
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
        50905,      -- Cholesterol, LDL, Calculated (Friedewald) - most common
        50906       -- Cholesterol, LDL, Measured (Direct assay) - when calculated invalid
        -- Both ITEMIDs are combined: whichever is closest chronologically
        -- is selected, regardless of measurement method
        -- =====================================================================
    )
    -- =========================================================================
    -- VALUE RANGE FILTERS - Edit these thresholds as needed
    -- =========================================================================
    AND le.valuenum > 0         -- Lower limit: lab values must be positive
    AND le.valuenum <= 400      -- Upper limit: filters extreme outliers (mg/dL)
    -- Normal range: <100 mg/dL (optimal) to ~190 mg/dL (very high)
    -- Values >400 mg/dL are extremely rare and likely data entry errors
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

    -- Closest LDL cholesterol measurement (within hospital admission)
    lm.valuenum AS ldl_first,
    lm.charttime AS ldl_first_charttime,
    lm.itemid AS ldl_first_itemid,

    -- Time from ICU admission to measurement (in minutes)
    -- IMPORTANT: Can be NEGATIVE (measured before ICU) or POSITIVE (measured after ICU)
    -- This is different from routine labs which are typically within ±6 hours
    lm.seconds_from_intime / 60.0 AS ldl_first_minutes_from_intime

FROM icustays ie
LEFT JOIN ldl_measurements lm
    ON ie.icustay_id = lm.icustay_id
    AND lm.rn = 1  -- Only the closest measurement (by absolute time distance)
ORDER BY ie.icustay_id;


-- ===============================================================================
-- DIAGNOSTIC QUERIES
-- ===============================================================================
-- These queries help assess data completeness and quality.
-- Uncomment and run as needed for data validation.

-- Check data completeness: How many ICU stays have LDL measurements?
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     SUM(CASE WHEN ldl_first IS NOT NULL THEN 1 ELSE 0 END) AS stays_with_ldl,
--     ROUND(100.0 * SUM(CASE WHEN ldl_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_ldl
-- FROM icu_first_ldl;

-- Distribution of LDL values
-- SELECT
--     MIN(ldl_first) AS min_ldl,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ldl_first) AS p25_ldl,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ldl_first) AS median_ldl,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ldl_first) AS p75_ldl,
--     MAX(ldl_first) AS max_ldl
-- FROM icu_first_ldl
-- WHERE ldl_first IS NOT NULL;

-- Check ITEMID distribution: Calculated vs Measured LDL
-- SELECT
--     ldl_first_itemid,
--     CASE
--         WHEN ldl_first_itemid = 50905 THEN 'Calculated (Friedewald)'
--         WHEN ldl_first_itemid = 50906 THEN 'Measured (Direct Assay)'
--         ELSE 'Unknown'
--     END AS measurement_method,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_ldl
-- WHERE ldl_first_itemid IS NOT NULL
-- GROUP BY ldl_first_itemid
-- ORDER BY n_stays DESC;

-- Timing analysis: When are closest LDL measurements typically taken?
-- Note: This shows the temporal distribution relative to ICU admission
-- Negative values = measured BEFORE ICU admission
-- SELECT
--     CASE
--         WHEN ldl_first_minutes_from_intime < -4320 THEN 'More than 3 days before ICU'
--         WHEN ldl_first_minutes_from_intime < -1440 THEN '1-3 days before ICU'
--         WHEN ldl_first_minutes_from_intime < -360 THEN '6-24 hours before ICU'
--         WHEN ldl_first_minutes_from_intime < 0 THEN 'Within 6h before ICU'
--         WHEN ldl_first_minutes_from_intime <= 360 THEN 'Within 6h after ICU'
--         WHEN ldl_first_minutes_from_intime <= 1440 THEN '6-24 hours after ICU'
--         WHEN ldl_first_minutes_from_intime <= 4320 THEN '1-3 days after ICU'
--         ELSE 'More than 3 days after ICU'
--     END AS timing_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_ldl
-- WHERE ldl_first IS NOT NULL
-- GROUP BY timing_category
-- ORDER BY MIN(ldl_first_minutes_from_intime);

-- Check LDL level categories (cardiovascular risk assessment)
-- SELECT
--     CASE
--         WHEN ldl_first < 100 THEN 'Optimal (< 100)'
--         WHEN ldl_first < 130 THEN 'Near Optimal (100-129)'
--         WHEN ldl_first < 160 THEN 'Borderline High (130-159)'
--         WHEN ldl_first < 190 THEN 'High (160-189)'
--         ELSE 'Very High (≥ 190)'
--     END AS ldl_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_ldl
-- WHERE ldl_first IS NOT NULL
-- GROUP BY ldl_category
-- ORDER BY MIN(ldl_first);
