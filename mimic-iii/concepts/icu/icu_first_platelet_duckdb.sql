-- ===============================================================================
-- MIMIC-III DuckDB: First Platelet Count per ICU Stay
-- ===============================================================================
-- This query creates a table with the first Platelet Count measurement
-- for each ICU stay in the MIMIC-III database, optimized for DuckDB.
--
-- The table includes:
--   - First Platelet Count value within time window (K/uL)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source)
--   - Minutes from ICU admission to measurement
--
-- ITEMID Configuration:
--   To modify which ITEMIDs are considered for Platelet Count, edit the list
--   in the WHERE clause within the plt_measurements CTE below.
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - No filters by default - includes all ICU stays for maximum flexibility
--   - Tracks ITEMID to identify measurement source
--   - NULL values indicate no Platelet Count measurement during the defined time window
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
--   Platelet Count uses a bounded window specific to each ICU stay:
--
--   Rationale:
--   - Platelet Count is a ROUTINE lab, measured very frequently in ICU
--   - We want the ICU admission baseline (or immediately preceding value)
--   - Start: intime - 6 hours captures pre-ICU labs (ED, floor) that reflect admission state
--   - End: outtime ensures we only capture labs from THIS ICU stay, preventing contamination
--   - Without outtime bound, joining on subject_id could capture labs from future ICU stays
--   - This provides clean temporal boundaries for each ICU episode
--
--   Implementation:
--   - Join on subject_id (not hadm_id, as Platelet Count can be checked across admissions)
--   - Order by charttime (first chronologically within window)
--   - Bounded to [intime - 6h, outtime] to prevent inter-stay contamination
--
-- CLINICAL CONTEXT:
--   Platelets (thrombocytes) are small blood cells essential for blood clotting and
--   hemostasis. Platelet count is a critical component of the Complete Blood Count (CBC)
--   and is routinely measured in ICU patients to assess bleeding risk and coagulation status.
--
--   Part of Complete Blood Count (CBC) and ROUTINELY measured in ICU.
--
--   Typical use cases in ICU:
--   - Bleeding risk assessment
--   - Coagulation monitoring
--   - Disseminated intravascular coagulation (DIC) detection
--   - Heparin-induced thrombocytopenia (HIT) monitoring
--   - Sepsis-associated thrombocytopenia
--   - Post-transfusion monitoring
--   - Medication-induced thrombocytopenia screening
--   - Part of SOFA score calculation (Sequential Organ Failure Assessment)
--   - Pre-procedure risk assessment (surgery, invasive procedures)
--
--   Note: Platelet count can be affected by:
--   - Bone marrow disorders (decreased production)
--   - Splenic sequestration (splenomegaly)
--   - Immune destruction (ITP, drug-induced)
--   - Consumption (DIC, TTP, HUS)
--   - Dilution (massive transfusion, fluid resuscitation)
--   - Heparin-induced thrombocytopenia (HIT)
--   - Sepsis and critical illness
--   - Medications (chemotherapy, antibiotics, anticonvulsants)
--
--   Reference Ranges:
--   - Normal: 150-400 K/uL (thousands per microliter)
--   - Mild Thrombocytopenia: 100-150 K/uL
--   - Moderate Thrombocytopenia: 50-100 K/uL
--   - Severe Thrombocytopenia: 20-50 K/uL
--   - Critical Thrombocytopenia: <20 K/uL
--   - Thrombocytosis: >400 K/uL
--
--   Clinical Interpretation - Thrombocytopenia (Low Platelets):
--   - 100-150 K/uL: Mild, usually no bleeding risk
--   - 50-100 K/uL: Moderate, increased bleeding with trauma/surgery
--   - 20-50 K/uL: Severe, spontaneous bleeding risk
--   - <20 K/uL: Critical, high risk spontaneous bleeding, transfusion often needed
--   - <10 K/uL: Life-threatening, immediate transfusion usually required
--
--   Clinical Interpretation - Thrombocytosis (High Platelets):
--   - 400-600 K/uL: Mild elevation, often reactive
--   - 600-1000 K/uL: Moderate elevation, monitor for thrombosis
--   - >1000 K/uL: Severe, increased thrombosis risk
--
--   Transfusion Thresholds:
--   - <10 K/uL: Prophylactic transfusion usually recommended
--   - <20 K/uL: Transfusion for bleeding or before invasive procedures
--   - <50 K/uL: Transfusion before major surgery or active bleeding
--   - <100 K/uL: Transfusion for neurosurgery or ophthalmologic surgery
--
--   SOFA Score (Coagulation Component):
--   - Platelets ≥150 K/uL: 0 points (normal)
--   - Platelets 100-149 K/uL: 1 point
--   - Platelets 50-99 K/uL: 2 points
--   - Platelets 20-49 K/uL: 3 points
--   - Platelets <20 K/uL: 4 points (severe organ dysfunction)
--
--   Critical Values:
--   - <20 K/uL: Severe bleeding risk, urgent intervention
--   - >1000 K/uL: Thrombosis risk, consider antiplatelet therapy
-- ===============================================================================

DROP TABLE IF EXISTS icu_first_platelet;

CREATE TABLE icu_first_platelet AS
WITH plt_measurements AS (
    -- Extract first Platelet Count measurement within ICU stay time window
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
        51265       -- Platelet Count (Hematology - CBC) - (~778k measurements)
        -- =====================================================================
    )
    -- =========================================================================
    -- VALUE RANGE FILTERS - Edit these thresholds as needed
    -- =========================================================================
    AND le.valuenum > 0         -- Lower limit: lab values must be positive
    AND le.valuenum <= 10000    -- Upper limit: filters extreme outliers (K/uL)
    -- Normal range: 150-400 K/uL
    -- Severe thrombocytopenia: <20 K/uL
    -- Thrombocytosis: >400 K/uL
    -- Values >10000 K/uL are physiologically impossible (measurement errors)
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

    -- First Platelet Count measurement (within ICU stay bounded window)
    pm.valuenum AS platelet_first,
    pm.charttime AS platelet_first_charttime,
    pm.itemid AS platelet_first_itemid,

    -- Time from ICU admission to measurement (in minutes)
    -- Can be NEGATIVE (measured before ICU admission, within -6h window)
    -- or POSITIVE (measured during ICU stay)
    pm.seconds_from_intime / 60.0 AS platelet_first_minutes_from_intime

FROM icustays ie
LEFT JOIN plt_measurements pm
    ON ie.icustay_id = pm.icustay_id
    AND pm.rn = 1  -- Only the first measurement (chronologically)
ORDER BY ie.icustay_id;


-- ===============================================================================
-- DIAGNOSTIC QUERIES
-- ===============================================================================
-- These queries help assess data completeness and quality.
-- Uncomment and run as needed for data validation.

-- Check data completeness: How many ICU stays have Platelet Count measurements?
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     SUM(CASE WHEN platelet_first IS NOT NULL THEN 1 ELSE 0 END) AS stays_with_platelet,
--     ROUND(100.0 * SUM(CASE WHEN platelet_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_platelet
-- FROM icu_first_platelet;

-- Distribution of Platelet Count values
-- SELECT
--     MIN(platelet_first) AS min_plt,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY platelet_first) AS p25_plt,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY platelet_first) AS median_plt,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY platelet_first) AS p75_plt,
--     MAX(platelet_first) AS max_plt
-- FROM icu_first_platelet
-- WHERE platelet_first IS NOT NULL;

-- Check ITEMID distribution
-- SELECT
--     platelet_first_itemid,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_platelet
-- WHERE platelet_first_itemid IS NOT NULL
-- GROUP BY platelet_first_itemid
-- ORDER BY n_stays DESC;

-- Timing analysis: When are first Platelet measurements typically taken?
-- SELECT
--     CASE
--         WHEN platelet_first_minutes_from_intime < -300 THEN '5-6 hours before ICU'
--         WHEN platelet_first_minutes_from_intime < -240 THEN '4-5 hours before ICU'
--         WHEN platelet_first_minutes_from_intime < -180 THEN '3-4 hours before ICU'
--         WHEN platelet_first_minutes_from_intime < -120 THEN '2-3 hours before ICU'
--         WHEN platelet_first_minutes_from_intime < -60 THEN '1-2 hours before ICU'
--         WHEN platelet_first_minutes_from_intime < 0 THEN 'Within 1h before ICU'
--         WHEN platelet_first_minutes_from_intime <= 60 THEN 'Within 1h after ICU'
--         WHEN platelet_first_minutes_from_intime <= 360 THEN '1-6 hours after ICU'
--         WHEN platelet_first_minutes_from_intime <= 1440 THEN '6-24 hours after ICU'
--         ELSE 'More than 24h after ICU'
--     END AS timing_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_platelet
-- WHERE platelet_first IS NOT NULL
-- GROUP BY timing_category
-- ORDER BY MIN(platelet_first_minutes_from_intime);

-- Thrombocytopenia severity assessment
-- SELECT
--     CASE
--         WHEN platelet_first < 10 THEN 'Life-threatening (< 10) - Immediate transfusion'
--         WHEN platelet_first < 20 THEN 'Critical (10-19) - High bleeding risk'
--         WHEN platelet_first < 50 THEN 'Severe (20-49) - Spontaneous bleeding risk'
--         WHEN platelet_first < 100 THEN 'Moderate (50-99) - Procedural risk'
--         WHEN platelet_first < 150 THEN 'Mild (100-149) - Monitor'
--         WHEN platelet_first <= 400 THEN 'Normal (150-400)'
--         WHEN platelet_first <= 600 THEN 'Mild Thrombocytosis (401-600)'
--         WHEN platelet_first <= 1000 THEN 'Moderate Thrombocytosis (601-1000)'
--         ELSE 'Severe Thrombocytosis (> 1000) - Thrombosis risk'
--     END AS platelet_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_platelet
-- WHERE platelet_first IS NOT NULL
-- GROUP BY platelet_category
-- ORDER BY MIN(platelet_first);

-- SOFA Score coagulation component
-- SELECT
--     CASE
--         WHEN platelet_first >= 150 THEN 'SOFA 0: Normal (≥ 150)'
--         WHEN platelet_first >= 100 THEN 'SOFA 1: Mild dysfunction (100-149)'
--         WHEN platelet_first >= 50 THEN 'SOFA 2: Moderate dysfunction (50-99)'
--         WHEN platelet_first >= 20 THEN 'SOFA 3: Severe dysfunction (20-49)'
--         ELSE 'SOFA 4: Critical dysfunction (< 20)'
--     END AS sofa_coagulation,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_platelet
-- WHERE platelet_first IS NOT NULL
-- GROUP BY sofa_coagulation
-- ORDER BY MIN(platelet_first) DESC;

-- Transfusion threshold analysis
-- SELECT
--     CASE
--         WHEN platelet_first < 10 THEN 'Urgent transfusion (< 10) - Life-threatening'
--         WHEN platelet_first < 20 THEN 'Transfusion likely (10-19) - High risk'
--         WHEN platelet_first < 50 THEN 'Transfusion if bleeding/procedure (20-49)'
--         WHEN platelet_first < 100 THEN 'Transfusion for major surgery (50-99)'
--         ELSE 'No routine transfusion needed (≥ 100)'
--     END AS transfusion_guidance,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
--     ROUND(AVG(platelet_first), 1) AS avg_platelet
-- FROM icu_first_platelet
-- WHERE platelet_first IS NOT NULL
-- GROUP BY transfusion_guidance
-- ORDER BY MIN(platelet_first);

-- Analyze measurements captured before vs during ICU stay
-- SELECT
--     CASE
--         WHEN platelet_first_minutes_from_intime < 0 THEN 'Before ICU admission (within -6h window)'
--         ELSE 'During ICU stay'
--     END AS measurement_timing,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
--     ROUND(AVG(platelet_first), 1) AS avg_platelet,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY platelet_first), 1) AS median_platelet
-- FROM icu_first_platelet
-- WHERE platelet_first IS NOT NULL
-- GROUP BY measurement_timing
-- ORDER BY MIN(platelet_first_minutes_from_intime);
