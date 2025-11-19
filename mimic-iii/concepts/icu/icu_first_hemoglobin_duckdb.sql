-- ===============================================================================
-- MIMIC-III DuckDB: First Hemoglobin per ICU Stay
-- ===============================================================================
-- This query creates a table with the first Hemoglobin measurement
-- for each ICU stay in the MIMIC-III database, optimized for DuckDB.
--
-- The table includes:
--   - First Hemoglobin value within time window (g/dL)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source)
--   - Minutes from ICU admission to measurement
--
-- ITEMID Configuration:
--   To modify which ITEMIDs are considered for Hemoglobin, edit the list
--   in the WHERE clause within the hgb_measurements CTE below.
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - No filters by default - includes all ICU stays for maximum flexibility
--   - Tracks ITEMID to identify measurement source
--   - NULL values indicate no Hemoglobin measurement during the defined time window
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
--   Unlike sparse labs (lipid panels) which use "closest within hospital admission"
--   approach, Hemoglobin uses a bounded window specific to each ICU stay:
--
--   Rationale:
--   - Hemoglobin is a ROUTINE lab, measured VERY frequently in ICU (highest coverage: 97.12%)
--   - We want the ICU admission baseline (or immediately preceding value)
--   - Start: intime - 6 hours captures pre-ICU labs (ED, floor) that reflect admission state
--   - End: outtime ensures we only capture labs from THIS ICU stay, preventing contamination
--   - Without outtime bound, joining on subject_id could capture labs from future ICU stays
--   - This provides clean temporal boundaries for each ICU episode
--
--   Implementation:
--   - Join on subject_id (not hadm_id, as Hemoglobin can be checked across admissions)
--   - Order by charttime (first chronologically within window)
--   - Bounded to [intime - 6h, outtime] to prevent inter-stay contamination
--   - Time offset exposed in output (minutes from ICU admission)
--
-- CLINICAL CONTEXT:
--   Hemoglobin (Hgb or Hb) is the iron-containing oxygen-transport protein in red
--   blood cells. It carries oxygen from the lungs to tissues and returns carbon
--   dioxide from tissues to the lungs. Hemoglobin is a fundamental component of
--   the Complete Blood Count (CBC), one of the most frequently ordered lab tests in ICU.
--
--   Part of Complete Blood Count (CBC) and ROUTINELY measured in ICU.
--
--   Typical use cases in ICU:
--   - Anemia detection and monitoring
--   - Oxygen-carrying capacity assessment
--   - Blood loss monitoring (trauma, surgery, GI bleeding)
--   - Transfusion decision-making (trigger thresholds)
--   - Part of admission workup for all ICU patients
--   - Daily monitoring in critically ill patients
--   - Polycythemia (elevated red blood cells) detection
--
--   Hemoglobin Measurement Sources:
--   - CBC (Complete Blood Count) - Most common, itemid 51222
--   - Blood Gas Analysis - Also includes Hgb, itemid 50811
--   Both measure the same parameter but are from different lab panels
--
--   Note: Hemoglobin levels can be affected by:
--   - Acute blood loss (trauma, surgery, GI bleeding)
--   - Chronic anemia (iron deficiency, chronic disease, hemolysis)
--   - Dehydration (hemoconcentration - falsely elevated)
--   - Fluid overload (hemodilution - falsely lowered)
--   - Transfusions (increases Hgb)
--   - Bone marrow disorders
--   - Nutritional deficiencies (iron, B12, folate)
--   - Chronic kidney disease (decreased erythropoietin)
--
--   Reference Ranges:
--   - Adult Males: 13.5-17.5 g/dL
--   - Adult Females: 12.0-15.5 g/dL
--   - Elderly: May be slightly lower (11.0-15.0 g/dL)
--   - Pregnancy: 11.0-14.0 g/dL (physiologic anemia)
--
--   Clinical Interpretation:
--   - Severe Anemia: <7.0 g/dL (often transfusion threshold in ICU)
--   - Moderate Anemia: 7.0-10.0 g/dL
--   - Mild Anemia: 10.0-12.0 g/dL (females), 10.0-13.5 g/dL (males)
--   - Normal: 12.0-15.5 g/dL (females), 13.5-17.5 g/dL (males)
--   - Polycythemia: >17.5 g/dL (males), >15.5 g/dL (females)
--
--   Transfusion Thresholds (varies by patient and condition):
--   - Restrictive strategy: Transfuse if Hgb <7.0 g/dL (most ICU patients)
--   - Liberal strategy: Transfuse if Hgb <8.0-10.0 g/dL (acute coronary syndrome, severe sepsis)
--   - Symptomatic anemia: Consider transfusion even at higher Hgb levels
--
--   Related Measures:
--   - Hematocrit (Hct): Percentage of blood volume occupied by RBCs
--     * Rule of thumb: Hct ≈ 3 × Hgb (e.g., Hgb 10 → Hct ~30%)
--   - Red Blood Cell Count (RBC): Number of red blood cells
--   - MCV, MCH, MCHC: Indices describing red blood cell characteristics
--
--   Critical Values (require immediate attention):
--   - <5.0 g/dL: Life-threatening anemia, immediate transfusion likely needed
--   - >20.0 g/dL: Severe polycythemia, risk of hyperviscosity syndrome
-- ===============================================================================

DROP TABLE IF EXISTS icu_first_hemoglobin;

CREATE TABLE icu_first_hemoglobin AS
WITH hgb_measurements AS (
    -- Extract first Hemoglobin measurement within ICU stay time window
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
        51222,      -- Hemoglobin (Hematology - CBC) - Most common (~752k measurements)
        50811       -- Hemoglobin (Blood Gas) - Also common (~89k measurements)
        -- Both measure the same parameter from different lab panels
        -- =====================================================================
    )
    -- =========================================================================
    -- VALUE RANGE FILTERS - Edit these thresholds as needed
    -- =========================================================================
    AND le.valuenum > 0         -- Lower limit: lab values must be positive
    AND le.valuenum <= 50       -- Upper limit: filters extreme outliers (g/dL)
    -- Normal range: 12.0-17.5 g/dL (varies by sex)
    -- Severe anemia: <7.0 g/dL
    -- Polycythemia: >17.5 g/dL
    -- Values >50 g/dL are physiologically impossible (measurement errors)
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

    -- First Hemoglobin measurement (within ICU stay bounded window)
    hm.valuenum AS hemoglobin_first,
    hm.charttime AS hemoglobin_first_charttime,
    hm.itemid AS hemoglobin_first_itemid,

    -- Time from ICU admission to measurement (in minutes)
    -- Can be NEGATIVE (measured before ICU admission, within -6h window)
    -- or POSITIVE (measured during ICU stay)
    hm.seconds_from_intime / 60.0 AS hemoglobin_first_minutes_from_intime

FROM icustays ie
LEFT JOIN hgb_measurements hm
    ON ie.icustay_id = hm.icustay_id
    AND hm.rn = 1  -- Only the first measurement (chronologically)
ORDER BY ie.icustay_id;


-- ===============================================================================
-- DIAGNOSTIC QUERIES
-- ===============================================================================
-- These queries help assess data completeness and quality.
-- Uncomment and run as needed for data validation.

-- Check data completeness: How many ICU stays have Hemoglobin measurements?
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     SUM(CASE WHEN hemoglobin_first IS NOT NULL THEN 1 ELSE 0 END) AS stays_with_hemoglobin,
--     ROUND(100.0 * SUM(CASE WHEN hemoglobin_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_hemoglobin
-- FROM icu_first_hemoglobin;

-- Distribution of Hemoglobin values
-- SELECT
--     MIN(hemoglobin_first) AS min_hgb,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY hemoglobin_first) AS p25_hgb,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY hemoglobin_first) AS median_hgb,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY hemoglobin_first) AS p75_hgb,
--     MAX(hemoglobin_first) AS max_hgb
-- FROM icu_first_hemoglobin
-- WHERE hemoglobin_first IS NOT NULL;

-- Check ITEMID distribution: CBC vs Blood Gas
-- SELECT
--     hemoglobin_first_itemid,
--     CASE
--         WHEN hemoglobin_first_itemid = 51222 THEN 'Hemoglobin (CBC/Hematology)'
--         WHEN hemoglobin_first_itemid = 50811 THEN 'Hemoglobin (Blood Gas)'
--     END AS measurement_source,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_hemoglobin
-- WHERE hemoglobin_first_itemid IS NOT NULL
-- GROUP BY hemoglobin_first_itemid
-- ORDER BY n_stays DESC;

-- Timing analysis: When are first Hemoglobin measurements typically taken relative to ICU admission?
-- SELECT
--     CASE
--         WHEN hemoglobin_first_minutes_from_intime < -300 THEN '5-6 hours before ICU'
--         WHEN hemoglobin_first_minutes_from_intime < -240 THEN '4-5 hours before ICU'
--         WHEN hemoglobin_first_minutes_from_intime < -180 THEN '3-4 hours before ICU'
--         WHEN hemoglobin_first_minutes_from_intime < -120 THEN '2-3 hours before ICU'
--         WHEN hemoglobin_first_minutes_from_intime < -60 THEN '1-2 hours before ICU'
--         WHEN hemoglobin_first_minutes_from_intime < 0 THEN 'Within 1h before ICU'
--         WHEN hemoglobin_first_minutes_from_intime <= 60 THEN 'Within 1h after ICU'
--         WHEN hemoglobin_first_minutes_from_intime <= 360 THEN '1-6 hours after ICU'
--         WHEN hemoglobin_first_minutes_from_intime <= 1440 THEN '6-24 hours after ICU'
--         ELSE 'More than 24h after ICU'
--     END AS timing_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_hemoglobin
-- WHERE hemoglobin_first IS NOT NULL
-- GROUP BY timing_category
-- ORDER BY MIN(hemoglobin_first_minutes_from_intime);

-- Check Hemoglobin level categories (anemia assessment)
-- SELECT
--     CASE
--         WHEN hemoglobin_first < 5.0 THEN 'Critical Anemia (< 5.0) - Life-threatening'
--         WHEN hemoglobin_first < 7.0 THEN 'Severe Anemia (5.0-6.9) - Transfusion threshold'
--         WHEN hemoglobin_first < 10.0 THEN 'Moderate Anemia (7.0-9.9)'
--         WHEN hemoglobin_first < 12.0 THEN 'Mild Anemia (10.0-11.9)'
--         WHEN hemoglobin_first <= 17.5 THEN 'Normal (12.0-17.5)'
--         ELSE 'Polycythemia (> 17.5) - Elevated'
--     END AS hemoglobin_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_hemoglobin
-- WHERE hemoglobin_first IS NOT NULL
-- GROUP BY hemoglobin_category
-- ORDER BY MIN(hemoglobin_first);

-- Transfusion threshold analysis (restrictive strategy)
-- SELECT
--     CASE
--         WHEN hemoglobin_first < 7.0 THEN 'Below Transfusion Threshold (< 7.0) - Consider transfusion'
--         WHEN hemoglobin_first < 10.0 THEN 'Low but above threshold (7.0-9.9) - Monitor closely'
--         ELSE 'Adequate (≥ 10.0) - No routine transfusion needed'
--     END AS transfusion_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
--     ROUND(AVG(hemoglobin_first), 2) AS avg_hgb
-- FROM icu_first_hemoglobin
-- WHERE hemoglobin_first IS NOT NULL
-- GROUP BY transfusion_category
-- ORDER BY MIN(hemoglobin_first);

-- Gender-specific anemia assessment (requires patients table join)
-- SELECT
--     p.gender,
--     CASE
--         WHEN p.gender = 'M' AND hemoglobin_first < 13.5 THEN 'Anemic (Male < 13.5)'
--         WHEN p.gender = 'F' AND hemoglobin_first < 12.0 THEN 'Anemic (Female < 12.0)'
--         ELSE 'Normal for gender'
--     END AS gender_specific_status,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY p.gender), 2) AS pct_within_gender,
--     ROUND(AVG(hemoglobin_first), 2) AS avg_hgb
-- FROM icu_first_hemoglobin hgb
-- INNER JOIN icustays ie ON hgb.icustay_id = ie.icustay_id
-- INNER JOIN patients p ON ie.subject_id = p.subject_id
-- WHERE hemoglobin_first IS NOT NULL
-- GROUP BY p.gender, gender_specific_status
-- ORDER BY p.gender, MIN(hemoglobin_first);

-- Analyze measurements captured before vs during ICU stay
-- SELECT
--     CASE
--         WHEN hemoglobin_first_minutes_from_intime < 0 THEN 'Before ICU admission (within -6h window)'
--         ELSE 'During ICU stay'
--     END AS measurement_timing,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
--     ROUND(AVG(hemoglobin_first), 2) AS avg_hgb,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY hemoglobin_first), 2) AS median_hgb
-- FROM icu_first_hemoglobin
-- WHERE hemoglobin_first IS NOT NULL
-- GROUP BY measurement_timing
-- ORDER BY MIN(hemoglobin_first_minutes_from_intime);
