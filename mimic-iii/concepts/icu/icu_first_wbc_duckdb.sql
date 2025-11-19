-- ===============================================================================
-- MIMIC-III DuckDB: First WBC (White Blood Cell Count) per ICU Stay
-- ===============================================================================
-- This query creates a table with the first WBC (White Blood Cell Count) measurement
-- for each ICU stay in the MIMIC-III database, optimized for DuckDB.
--
-- The table includes:
--   - First WBC value within time window (K/uL)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source)
--   - Minutes from ICU admission to measurement
--
-- ITEMID Configuration:
--   To modify which ITEMIDs are considered for WBC, edit the list
--   in the WHERE clause within the wbc_measurements CTE below.
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - No filters by default - includes all ICU stays for maximum flexibility
--   - Tracks ITEMID to identify measurement source
--   - NULL values indicate no WBC measurement during the defined time window
--   - Uses labevents table (laboratory values, not vital signs)
--   - Part of Complete Blood Count (CBC)
--
-- Unit of Analysis: ICU stays (icustay_id)
--   - Each ICU stay is analyzed independently
--   - One patient can have multiple hospital admissions
--   - One hospital admission can have multiple ICU stays
--   - We find the "first" measurement for EACH ICU stay
--
-- DECISION: Time Window from "intime - 6 hours" to "outtime" (ICU Stay Bounded)
--   WBC Count uses a bounded window specific to each ICU stay:
--
--   Rationale:
--   - WBC Count is a ROUTINE lab, measured very frequently in ICU
--   - We want the ICU admission baseline (or immediately preceding value)
--   - Start: intime - 6 hours captures pre-ICU labs (ED, floor) that reflect admission state
--   - End: outtime ensures we only capture labs from THIS ICU stay, preventing contamination
--   - Without outtime bound, joining on subject_id could capture labs from future ICU stays
--   - This provides clean temporal boundaries for each ICU episode
--
--   Implementation:
--   - Join on subject_id (not hadm_id, as WBC can be checked across admissions)
--   - Order by charttime (first chronologically within window)
--   - Bounded to [intime - 6h, outtime] to prevent inter-stay contamination
--
-- CLINICAL CONTEXT:
--   White Blood Cells (WBCs or leukocytes) are immune system cells that fight infection
--   and foreign invaders. WBC count is a critical component of the Complete Blood Count (CBC)
--   and is routinely measured in ICU patients to detect infection, inflammation, and
--   immune system disorders.
--
--   Part of Complete Blood Count (CBC) and ROUTINELY measured in ICU.
--
--   Typical use cases in ICU:
--   - Infection detection and monitoring (sepsis, pneumonia, UTI)
--   - Inflammatory response assessment
--   - Immune system function evaluation
--   - Bone marrow disorder detection
--   - Medication effect monitoring (immunosuppression, chemotherapy)
--   - Part of LODS score calculation (Logistic Organ Dysfunction System)
--   - Leukemia and hematologic malignancy screening
--   - Post-transplant monitoring
--
--   Note: WBC count can be affected by:
--   - Infection (bacterial, viral, fungal)
--   - Inflammation (trauma, surgery, autoimmune)
--   - Stress response (critical illness, pain)
--   - Medications (steroids increase, chemotherapy decreases)
--   - Bone marrow disorders (leukemia, aplastic anemia)
--   - Immune disorders
--   - Splenic disorders
--
--   Reference Ranges:
--   - Normal: 4.5-11.0 K/uL (thousands per microliter)
--   - Leukopenia: <4.5 K/uL (low WBC)
--   - Leukocytosis: >11.0 K/uL (high WBC)
--
--   Clinical Interpretation - Leukopenia (Low WBC):
--   - 3.0-4.5 K/uL: Mild leukopenia, monitor
--   - 2.0-3.0 K/uL: Moderate leukopenia, infection risk
--   - 1.0-2.0 K/uL: Severe leukopenia, high infection risk
--   - <1.0 K/uL: Critical leukopenia (neutropenia), immediate intervention
--
--   Clinical Interpretation - Leukocytosis (High WBC):
--   - 11-15 K/uL: Mild elevation, common in stress/inflammation
--   - 15-25 K/uL: Moderate elevation, investigate infection
--   - 25-50 K/uL: Marked elevation, severe infection or leukemia
--   - >50 K/uL: Extreme elevation, leukemoid reaction or leukemia
--   - >100 K/uL: Hyperleukocytosis, oncologic emergency
--
--   Differential Diagnosis by WBC Level:
--   - Low WBC (<4.5): Viral infection, bone marrow failure, immunosuppression
--   - Normal WBC (4.5-11): Normal or early infection
--   - High WBC (>11): Bacterial infection, inflammation, stress, leukemia
--   - Very High (>25): Severe infection, leukemoid reaction, leukemia
--
--   LODS Score (Hematologic Component):
--   Uses both WBC and Platelets:
--   - WBC <1.0 K/uL: 3 points (severe)
--   - WBC <2.5 K/uL: 1 point
--   - WBC ≥50.0 K/uL: 1 point
--   - Combined with platelet count for full hematologic score
--
--   Critical Values:
--   - <1.0 K/uL: Severe neutropenia, high infection risk
--   - >50 K/uL: Possible leukemia or severe infection
--   - >100 K/uL: Hyperleukocytosis, oncologic emergency
-- ===============================================================================

DROP TABLE IF EXISTS icu_first_wbc;

CREATE TABLE icu_first_wbc AS
WITH wbc_measurements AS (
    -- Extract first WBC measurement within ICU stay time window
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
        51300,      -- WBC Count (Hematology - CBC) - (~27k measurements)
        51301       -- White Blood Cells (Hematology - CBC) - Duplicate of 51300 (~753k measurements)
        -- Both are the same measurement, 51301 is more common in MIMIC-III
        -- =====================================================================
    )
    -- =========================================================================
    -- VALUE RANGE FILTERS - Edit these thresholds as needed
    -- =========================================================================
    AND le.valuenum > 0         -- Lower limit: lab values must be positive
    AND le.valuenum <= 1000     -- Upper limit: filters extreme outliers (K/uL)
    -- Normal range: 4.5-11.0 K/uL
    -- Leukopenia: <4.5 K/uL
    -- Leukocytosis: >11.0 K/uL
    -- Values >1000 K/uL are physiologically impossible (measurement errors)
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

    -- First WBC measurement (within ICU stay bounded window)
    wm.valuenum AS wbc_first,
    wm.charttime AS wbc_first_charttime,
    wm.itemid AS wbc_first_itemid,

    -- Time from ICU admission to measurement (in minutes)
    -- Can be NEGATIVE (measured before ICU admission, within -6h window)
    -- or POSITIVE (measured during ICU stay)
    wm.seconds_from_intime / 60.0 AS wbc_first_minutes_from_intime

FROM icustays ie
LEFT JOIN wbc_measurements wm
    ON ie.icustay_id = wm.icustay_id
    AND wm.rn = 1  -- Only the first measurement (chronologically)
ORDER BY ie.icustay_id;


-- ===============================================================================
-- DIAGNOSTIC QUERIES
-- ===============================================================================
-- These queries help assess data completeness and quality.
-- Uncomment and run as needed for data validation.

-- Check data completeness: How many ICU stays have WBC measurements?
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     SUM(CASE WHEN wbc_first IS NOT NULL THEN 1 ELSE 0 END) AS stays_with_wbc,
--     ROUND(100.0 * SUM(CASE WHEN wbc_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_wbc
-- FROM icu_first_wbc;

-- Distribution of WBC values
-- SELECT
--     MIN(wbc_first) AS min_wbc,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY wbc_first) AS p25_wbc,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY wbc_first) AS median_wbc,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY wbc_first) AS p75_wbc,
--     MAX(wbc_first) AS max_wbc
-- FROM icu_first_wbc
-- WHERE wbc_first IS NOT NULL;

-- Check ITEMID distribution
-- SELECT
--     wbc_first_itemid,
--     CASE
--         WHEN wbc_first_itemid = 51300 THEN 'WBC Count'
--         WHEN wbc_first_itemid = 51301 THEN 'White Blood Cells (duplicate of 51300)'
--     END AS itemid_description,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_wbc
-- WHERE wbc_first_itemid IS NOT NULL
-- GROUP BY wbc_first_itemid
-- ORDER BY n_stays DESC;

-- Timing analysis: When are first WBC measurements typically taken?
-- SELECT
--     CASE
--         WHEN wbc_first_minutes_from_intime < -300 THEN '5-6 hours before ICU'
--         WHEN wbc_first_minutes_from_intime < -240 THEN '4-5 hours before ICU'
--         WHEN wbc_first_minutes_from_intime < -180 THEN '3-4 hours before ICU'
--         WHEN wbc_first_minutes_from_intime < -120 THEN '2-3 hours before ICU'
--         WHEN wbc_first_minutes_from_intime < -60 THEN '1-2 hours before ICU'
--         WHEN wbc_first_minutes_from_intime < 0 THEN 'Within 1h before ICU'
--         WHEN wbc_first_minutes_from_intime <= 60 THEN 'Within 1h after ICU'
--         WHEN wbc_first_minutes_from_intime <= 360 THEN '1-6 hours after ICU'
--         WHEN wbc_first_minutes_from_intime <= 1440 THEN '6-24 hours after ICU'
--         ELSE 'More than 24h after ICU'
--     END AS timing_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_wbc
-- WHERE wbc_first IS NOT NULL
-- GROUP BY timing_category
-- ORDER BY MIN(wbc_first_minutes_from_intime);

-- WBC level categories (infection/inflammation assessment)
-- SELECT
--     CASE
--         WHEN wbc_first < 1.0 THEN 'Critical Leukopenia (< 1.0) - Severe infection risk'
--         WHEN wbc_first < 2.0 THEN 'Severe Leukopenia (1.0-1.9) - High infection risk'
--         WHEN wbc_first < 3.0 THEN 'Moderate Leukopenia (2.0-2.9) - Infection risk'
--         WHEN wbc_first < 4.5 THEN 'Mild Leukopenia (3.0-4.4) - Monitor'
--         WHEN wbc_first <= 11.0 THEN 'Normal (4.5-11.0)'
--         WHEN wbc_first <= 15.0 THEN 'Mild Leukocytosis (11.1-15.0) - Stress/inflammation'
--         WHEN wbc_first <= 25.0 THEN 'Moderate Leukocytosis (15.1-25.0) - Infection likely'
--         WHEN wbc_first <= 50.0 THEN 'Marked Leukocytosis (25.1-50.0) - Severe infection'
--         WHEN wbc_first <= 100.0 THEN 'Extreme Leukocytosis (50.1-100.0) - Leukemia possible'
--         ELSE 'Hyperleukocytosis (> 100.0) - Oncologic emergency'
--     END AS wbc_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_wbc
-- WHERE wbc_first IS NOT NULL
-- GROUP BY wbc_category
-- ORDER BY MIN(wbc_first);

-- LODS Score hematologic component (partial - needs platelets too)
-- SELECT
--     CASE
--         WHEN wbc_first < 1.0 THEN 'LODS +3: Critical leukopenia (< 1.0)'
--         WHEN wbc_first < 2.5 THEN 'LODS +1: Severe leukopenia (1.0-2.4)'
--         WHEN wbc_first >= 50.0 THEN 'LODS +1: Extreme leukocytosis (≥ 50.0)'
--         ELSE 'LODS 0: Normal range for WBC component'
--     END AS lods_wbc_component,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_wbc
-- WHERE wbc_first IS NOT NULL
-- GROUP BY lods_wbc_component
-- ORDER BY MIN(wbc_first);

-- Clinical action guidance
-- SELECT
--     CASE
--         WHEN wbc_first < 1.0 THEN 'Critical (< 1.0) - Isolation, growth factors, urgent ID consult'
--         WHEN wbc_first < 2.5 THEN 'Severe (1.0-2.4) - Monitor closely, infection precautions'
--         WHEN wbc_first < 4.5 THEN 'Low (2.5-4.4) - Monitor, investigate cause'
--         WHEN wbc_first <= 11.0 THEN 'Normal (4.5-11.0) - Routine monitoring'
--         WHEN wbc_first <= 15.0 THEN 'Elevated (11.1-15.0) - Monitor, consider infection'
--         WHEN wbc_first <= 25.0 THEN 'High (15.1-25.0) - Investigate infection source'
--         WHEN wbc_first <= 50.0 THEN 'Very High (25.1-50.0) - Urgent workup, cultures'
--         ELSE 'Critical High (> 50.0) - Consider leukemia, hematology consult'
--     END AS clinical_action,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
--     ROUND(AVG(wbc_first), 1) AS avg_wbc
-- FROM icu_first_wbc
-- WHERE wbc_first IS NOT NULL
-- GROUP BY clinical_action
-- ORDER BY MIN(wbc_first);

-- Analyze measurements captured before vs during ICU stay
-- SELECT
--     CASE
--         WHEN wbc_first_minutes_from_intime < 0 THEN 'Before ICU admission (within -6h window)'
--         ELSE 'During ICU stay'
--     END AS measurement_timing,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
--     ROUND(AVG(wbc_first), 1) AS avg_wbc,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY wbc_first), 1) AS median_wbc
-- FROM icu_first_wbc
-- WHERE wbc_first IS NOT NULL
-- GROUP BY measurement_timing
-- ORDER BY MIN(wbc_first_minutes_from_intime);
