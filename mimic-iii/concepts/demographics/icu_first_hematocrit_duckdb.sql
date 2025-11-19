-- ===============================================================================
-- MIMIC-III DuckDB: First Hematocrit per ICU Stay
-- ===============================================================================
-- This query creates a table with the first Hematocrit measurement
-- for each ICU stay in the MIMIC-III database, optimized for DuckDB.
--
-- The table includes:
--   - First Hematocrit value within time window (%)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source)
--   - Minutes from ICU admission to measurement
--
-- ITEMID Configuration:
--   To modify which ITEMIDs are considered for Hematocrit, edit the list
--   in the WHERE clause within the hct_measurements CTE below.
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - No filters by default - includes all ICU stays for maximum flexibility
--   - Tracks ITEMID to identify measurement source
--   - NULL values indicate no Hematocrit measurement during the defined time window
--   - Uses labevents table (laboratory values, not vital signs)
--   - Part of Complete Blood Count (CBC) and Blood Gas panels
--
-- Unit of Analysis: ICU stays (icustay_id)
--   - Each ICU stay is analyzed independently
--   - One patient can have multiple hospital admissions
--   - One hospital admission can have multiple ICU stays
--   - We find the "first" measurement for EACH ICU stay
--
-- DECISION: Time Window from "intime - 6 hours" to "outtime" (ICU Stay Bounded)
--   Hematocrit uses a bounded window specific to each ICU stay:
--
--   Rationale:
--   - Hematocrit is a ROUTINE lab, measured very frequently in ICU
--   - We want the ICU admission baseline (or immediately preceding value)
--   - Start: intime - 6 hours captures pre-ICU labs (ED, floor) that reflect admission state
--   - End: outtime ensures we only capture labs from THIS ICU stay, preventing contamination
--   - Without outtime bound, joining on subject_id could capture labs from future ICU stays
--   - This provides clean temporal boundaries for each ICU episode
--
--   Implementation:
--   - Join on subject_id (not hadm_id, as Hematocrit can be checked across admissions)
--   - Order by charttime (first chronologically within window)
--   - Bounded to [intime - 6h, outtime] to prevent inter-stay contamination
--
-- CLINICAL CONTEXT:
--   Hematocrit (Hct) is the percentage of blood volume occupied by red blood cells.
--   It reflects the oxygen-carrying capacity of blood and is closely related to
--   hemoglobin. Hematocrit is a fundamental component of the Complete Blood Count (CBC),
--   one of the most frequently ordered lab tests in ICU.
--
--   Part of Complete Blood Count (CBC) and ROUTINELY measured in ICU.
--
--   Typical use cases in ICU:
--   - Anemia detection and monitoring (parallel to hemoglobin)
--   - Blood volume status assessment
--   - Dehydration vs fluid overload evaluation
--   - Blood loss monitoring (trauma, surgery, GI bleeding)
--   - Transfusion decision-making
--   - Polycythemia detection
--   - Part of admission workup for all ICU patients
--
--   Hematocrit Measurement Sources:
--   - CBC (Complete Blood Count) - Most common, itemid 51221
--   - Blood Gas Analysis - Calculated Hematocrit, itemid 50810
--   Both measure the same parameter but from different lab panels
--
--   Relationship to Hemoglobin:
--   - Rule of thumb: Hct ≈ 3 × Hemoglobin
--   - Example: Hemoglobin 12 g/dL → Hematocrit ~36%
--   - This relationship helps validate measurements
--
--   Note: Hematocrit levels can be affected by:
--   - Acute blood loss (decreases Hct)
--   - Dehydration (hemoconcentration - falsely elevated Hct)
--   - Fluid overload (hemodilution - falsely lowered Hct)
--   - Transfusions (increases Hct)
--   - Chronic anemia (decreased Hct)
--   - Polycythemia (increased Hct)
--
--   Reference Ranges:
--   - Adult Males: 40-54%
--   - Adult Females: 36-48%
--   - Elderly: May be slightly lower
--   - Pregnancy: 30-40% (physiologic anemia)
--
--   Clinical Interpretation:
--   - Severe Anemia: <21% (critical, transfusion usually needed)
--   - Moderate Anemia: 21-30%
--   - Mild Anemia: 30-36% (females), 30-40% (males)
--   - Normal: 36-48% (females), 40-54% (males)
--   - Polycythemia: >48% (females), >54% (males)
--   - Severe Polycythemia: >60% (hyperviscosity risk)
--
--   Transfusion Considerations:
--   - Hct <21% often indicates need for transfusion
--   - Hct 21-24% may require transfusion depending on symptoms
--   - Each unit of packed RBCs typically increases Hct by 3-4%
--
--   Critical Values:
--   - <15%: Life-threatening anemia
--   - >60%: Severe polycythemia, hyperviscosity syndrome risk
-- ===============================================================================

DROP TABLE IF EXISTS icu_first_hematocrit;

CREATE TABLE icu_first_hematocrit AS
WITH hct_measurements AS (
    -- Extract first Hematocrit measurement within ICU stay time window
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
        51221,      -- Hematocrit (Hematology - CBC) - Most common (~881k measurements)
        50810       -- Hematocrit, Calculated (Blood Gas) - (~89k measurements)
        -- Both measure the same parameter from different lab panels
        -- =====================================================================
    )
    -- =========================================================================
    -- VALUE RANGE FILTERS - Edit these thresholds as needed
    -- =========================================================================
    AND le.valuenum > 0         -- Lower limit: lab values must be positive
    AND le.valuenum <= 100      -- Upper limit: percentage cannot exceed 100%
    -- Normal range: 36-54% (varies by sex)
    -- Severe anemia: <21%
    -- Polycythemia: >54%
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

    -- First Hematocrit measurement (within ICU stay bounded window)
    hm.valuenum AS hematocrit_first,
    hm.charttime AS hematocrit_first_charttime,
    hm.itemid AS hematocrit_first_itemid,

    -- Time from ICU admission to measurement (in minutes)
    -- Can be NEGATIVE (measured before ICU admission, within -6h window)
    -- or POSITIVE (measured during ICU stay)
    hm.seconds_from_intime / 60.0 AS hematocrit_first_minutes_from_intime

FROM icustays ie
LEFT JOIN hct_measurements hm
    ON ie.icustay_id = hm.icustay_id
    AND hm.rn = 1  -- Only the first measurement (chronologically)
ORDER BY ie.icustay_id;


-- ===============================================================================
-- DIAGNOSTIC QUERIES
-- ===============================================================================
-- These queries help assess data completeness and quality.
-- Uncomment and run as needed for data validation.

-- Check data completeness: How many ICU stays have Hematocrit measurements?
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     SUM(CASE WHEN hematocrit_first IS NOT NULL THEN 1 ELSE 0 END) AS stays_with_hematocrit,
--     ROUND(100.0 * SUM(CASE WHEN hematocrit_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_hematocrit
-- FROM icu_first_hematocrit;

-- Distribution of Hematocrit values
-- SELECT
--     MIN(hematocrit_first) AS min_hct,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY hematocrit_first) AS p25_hct,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY hematocrit_first) AS median_hct,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY hematocrit_first) AS p75_hct,
--     MAX(hematocrit_first) AS max_hct
-- FROM icu_first_hematocrit
-- WHERE hematocrit_first IS NOT NULL;

-- Check ITEMID distribution: CBC vs Blood Gas
-- SELECT
--     hematocrit_first_itemid,
--     CASE
--         WHEN hematocrit_first_itemid = 51221 THEN 'Hematocrit (CBC/Hematology)'
--         WHEN hematocrit_first_itemid = 50810 THEN 'Hematocrit Calculated (Blood Gas)'
--     END AS measurement_source,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_hematocrit
-- WHERE hematocrit_first_itemid IS NOT NULL
-- GROUP BY hematocrit_first_itemid
-- ORDER BY n_stays DESC;

-- Timing analysis: When are first Hematocrit measurements typically taken?
-- SELECT
--     CASE
--         WHEN hematocrit_first_minutes_from_intime < -300 THEN '5-6 hours before ICU'
--         WHEN hematocrit_first_minutes_from_intime < -240 THEN '4-5 hours before ICU'
--         WHEN hematocrit_first_minutes_from_intime < -180 THEN '3-4 hours before ICU'
--         WHEN hematocrit_first_minutes_from_intime < -120 THEN '2-3 hours before ICU'
--         WHEN hematocrit_first_minutes_from_intime < -60 THEN '1-2 hours before ICU'
--         WHEN hematocrit_first_minutes_from_intime < 0 THEN 'Within 1h before ICU'
--         WHEN hematocrit_first_minutes_from_intime <= 60 THEN 'Within 1h after ICU'
--         WHEN hematocrit_first_minutes_from_intime <= 360 THEN '1-6 hours after ICU'
--         WHEN hematocrit_first_minutes_from_intime <= 1440 THEN '6-24 hours after ICU'
--         ELSE 'More than 24h after ICU'
--     END AS timing_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_hematocrit
-- WHERE hematocrit_first IS NOT NULL
-- GROUP BY timing_category
-- ORDER BY MIN(hematocrit_first_minutes_from_intime);

-- Check Hematocrit level categories (anemia/polycythemia assessment)
-- SELECT
--     CASE
--         WHEN hematocrit_first < 15 THEN 'Critical Anemia (< 15%) - Life-threatening'
--         WHEN hematocrit_first < 21 THEN 'Severe Anemia (15-20%) - Transfusion needed'
--         WHEN hematocrit_first < 30 THEN 'Moderate Anemia (21-29%)'
--         WHEN hematocrit_first < 36 THEN 'Mild Anemia (30-35%)'
--         WHEN hematocrit_first <= 54 THEN 'Normal (36-54%)'
--         WHEN hematocrit_first <= 60 THEN 'Polycythemia (55-60%)'
--         ELSE 'Severe Polycythemia (> 60%) - Hyperviscosity risk'
--     END AS hematocrit_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_hematocrit
-- WHERE hematocrit_first IS NOT NULL
-- GROUP BY hematocrit_category
-- ORDER BY MIN(hematocrit_first);

-- Validate Hct/Hgb relationship (requires icu_first_hemoglobin table)
-- Rule of thumb: Hct ≈ 3 × Hemoglobin
-- SELECT
--     COUNT(*) AS n_stays,
--     ROUND(AVG(hct.hematocrit_first / hgb.hemoglobin_first), 2) AS avg_hct_hgb_ratio,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY hct.hematocrit_first / hgb.hemoglobin_first), 2) AS median_hct_hgb_ratio,
--     SUM(CASE WHEN (hct.hematocrit_first / hgb.hemoglobin_first) BETWEEN 2.5 AND 3.5 THEN 1 ELSE 0 END) AS stays_within_expected_ratio,
--     ROUND(100.0 * SUM(CASE WHEN (hct.hematocrit_first / hgb.hemoglobin_first) BETWEEN 2.5 AND 3.5 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_within_expected
-- FROM icu_first_hematocrit hct
-- INNER JOIN icu_first_hemoglobin hgb
--     ON hct.icustay_id = hgb.icustay_id
-- WHERE hct.hematocrit_first IS NOT NULL
--   AND hgb.hemoglobin_first IS NOT NULL
--   AND hgb.hemoglobin_first > 0;

-- Analyze measurements captured before vs during ICU stay
-- SELECT
--     CASE
--         WHEN hematocrit_first_minutes_from_intime < 0 THEN 'Before ICU admission (within -6h window)'
--         ELSE 'During ICU stay'
--     END AS measurement_timing,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
--     ROUND(AVG(hematocrit_first), 2) AS avg_hct,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY hematocrit_first), 2) AS median_hct
-- FROM icu_first_hematocrit
-- WHERE hematocrit_first IS NOT NULL
-- GROUP BY measurement_timing
-- ORDER BY MIN(hematocrit_first_minutes_from_intime);
