-- ===============================================================================
-- MIMIC-III DuckDB: Primary Diagnosis and Condition Flags per ICU Stay
-- ===============================================================================
-- This query creates a table with primary diagnosis information and clinical
-- condition flags for each ICU stay in the MIMIC-III database.
--
-- The table includes:
--   - Primary diagnosis (seq_num = 1) with ICD-9 code and description
--   - Total diagnosis count for the hospital admission
--   - 15 binary condition flags based on ALL admission diagnoses
--
-- IMPORTANT NOTES:
--   - Diagnoses are linked at ADMISSION level (hadm_id), not ICU stay level
--   - Multiple ICU stays from same admission will have identical diagnosis data
--   - Primary diagnosis = seq_num 1 (administrative/billing primary diagnosis)
--   - Condition flags check ALL diagnoses for the admission (not just primary)
--
-- CONDITION FLAGS (based on ICD-9 codes):
--   Cardiovascular:
--     - chf: Congestive Heart Failure
--     - acute_mi: Acute Myocardial Infarction
--     - afib: Atrial Fibrillation/Flutter
--     - stroke: Stroke/Cerebrovascular Disease
--
--   Respiratory:
--     - pneumonia: Pneumonia
--     - copd: Chronic Obstructive Pulmonary Disease
--     - ards: Acute Respiratory Distress Syndrome
--     - respiratory_failure: Respiratory Failure
--
--   Renal:
--     - acute_renal_failure: Acute Kidney Injury
--     - chronic_renal_failure: Chronic Kidney Disease
--
--   Infection/Other:
--     - sepsis: Sepsis/Septicemia (simple approach)
--     - diabetes: Diabetes Mellitus
--     - liver_disease: Liver Disease
--     - malignancy: Cancer/Neoplasm
--     - trauma: Injury/Trauma
--
-- Unit of Analysis: ICU stays (icustay_id)
-- ===============================================================================

DROP TABLE IF EXISTS icu_primary_diagnosis;

CREATE TABLE icu_primary_diagnosis AS
WITH primary_diag AS (
    -- =========================================================================
    -- Extract primary diagnosis (seq_num = 1) for each hospital admission
    -- =========================================================================
    SELECT
        hadm_id,
        icd9_code,
        seq_num
    FROM diagnoses_icd
    WHERE seq_num = 1
),
diag_counts AS (
    -- =========================================================================
    -- Count total number of diagnoses per hospital admission
    -- =========================================================================
    SELECT
        hadm_id,
        COUNT(*) AS total_diagnoses
    FROM diagnoses_icd
    GROUP BY hadm_id
),
diag_flags AS (
    -- =========================================================================
    -- Create binary flags for 15 clinical conditions
    -- Checks ALL diagnoses for the admission (not just primary)
    -- Flag = 1 if ANY diagnosis matches the condition criteria
    -- =========================================================================
    SELECT
        hadm_id,

        -- =====================================================================
        -- CARDIOVASCULAR CONDITIONS
        -- =====================================================================

        -- Congestive Heart Failure (CHF)
        -- Based on Elixhauser comorbidity definitions
        MAX(CASE
            WHEN icd9_code = '39891' THEN 1
            WHEN icd9_code BETWEEN '4280' AND '4289' THEN 1
            WHEN icd9_code IN ('40201','40211','40291','40401','40403','40411','40413','40491','40493') THEN 1
            ELSE 0
        END) AS chf,

        -- Acute Myocardial Infarction (MI)
        MAX(CASE
            WHEN icd9_code LIKE '410%' THEN 1
            WHEN icd9_code = '412' THEN 1
            ELSE 0
        END) AS acute_mi,

        -- Atrial Fibrillation/Flutter
        MAX(CASE
            WHEN icd9_code LIKE '4273%' THEN 1
            ELSE 0
        END) AS afib,

        -- Stroke (Cerebrovascular Disease)
        MAX(CASE
            WHEN icd9_code LIKE '430%' THEN 1
            WHEN icd9_code LIKE '431%' THEN 1
            WHEN icd9_code LIKE '432%' THEN 1
            WHEN icd9_code LIKE '433%' THEN 1
            WHEN icd9_code LIKE '434%' THEN 1
            ELSE 0
        END) AS stroke,

        -- =====================================================================
        -- RESPIRATORY CONDITIONS
        -- =====================================================================

        -- Pneumonia
        -- Covers bacterial, viral, and unspecified pneumonia
        MAX(CASE
            WHEN icd9_code BETWEEN '480' AND '48099' THEN 1
            WHEN icd9_code BETWEEN '481' AND '48199' THEN 1
            WHEN icd9_code BETWEEN '482' AND '48299' THEN 1
            WHEN icd9_code BETWEEN '483' AND '48399' THEN 1
            WHEN icd9_code BETWEEN '484' AND '48499' THEN 1
            WHEN icd9_code BETWEEN '485' AND '48599' THEN 1
            WHEN icd9_code BETWEEN '486' AND '48699' THEN 1
            WHEN icd9_code BETWEEN '487' AND '48799' THEN 1
            WHEN icd9_code BETWEEN '488' AND '48899' THEN 1
            WHEN icd9_code = '486' THEN 1
            ELSE 0
        END) AS pneumonia,

        -- COPD (Chronic Obstructive Pulmonary Disease)
        -- Based on ALINE cohort definitions
        MAX(CASE
            WHEN icd9_code IN ('4660','490','4910','4911','49120','49121','4918','4919',
                               '4920','4928','494','4940','4941','496') THEN 1
            ELSE 0
        END) AS copd,

        -- ARDS (Acute Respiratory Distress Syndrome)
        MAX(CASE
            WHEN icd9_code = '51882' THEN 1
            WHEN icd9_code = '5185' THEN 1
            ELSE 0
        END) AS ards,

        -- Respiratory Failure
        MAX(CASE
            WHEN icd9_code LIKE '518%' THEN 1
            ELSE 0
        END) AS respiratory_failure,

        -- =====================================================================
        -- RENAL CONDITIONS
        -- =====================================================================

        -- Acute Renal Failure (Acute Kidney Injury)
        MAX(CASE
            WHEN icd9_code LIKE '584%' THEN 1
            ELSE 0
        END) AS acute_renal_failure,

        -- Chronic Renal Failure (Chronic Kidney Disease)
        MAX(CASE
            WHEN icd9_code LIKE '585%' THEN 1
            ELSE 0
        END) AS chronic_renal_failure,

        -- =====================================================================
        -- INFECTION
        -- =====================================================================

        -- Sepsis/Septicemia (Simple approach - explicit codes only)
        -- For more comprehensive detection, see concepts/sepsis/angus.sql
        MAX(CASE
            WHEN icd9_code LIKE '038%' THEN 1  -- Septicemia
            WHEN icd9_code = '99591' THEN 1    -- Sepsis
            WHEN icd9_code = '99592' THEN 1    -- Severe sepsis
            WHEN icd9_code = '78552' THEN 1    -- Septic shock
            ELSE 0
        END) AS sepsis,

        -- =====================================================================
        -- METABOLIC/ENDOCRINE
        -- =====================================================================

        -- Diabetes Mellitus (all types)
        MAX(CASE
            WHEN icd9_code LIKE '250%' THEN 1
            ELSE 0
        END) AS diabetes,

        -- =====================================================================
        -- HEPATIC
        -- =====================================================================

        -- Liver Disease (chronic liver disease and cirrhosis)
        MAX(CASE
            WHEN icd9_code LIKE '571%' THEN 1
            ELSE 0
        END) AS liver_disease,

        -- =====================================================================
        -- ONCOLOGIC
        -- =====================================================================

        -- Malignancy (neoplasms - includes remissions)
        -- ICD-9 codes 140-239 cover all neoplasms
        MAX(CASE
            WHEN icd9_code BETWEEN '140' AND '23999' THEN 1
            ELSE 0
        END) AS malignancy,

        -- =====================================================================
        -- TRAUMA
        -- =====================================================================

        -- Trauma/Injury
        -- ICD-9 codes 800-999 cover injury and poisoning
        MAX(CASE
            WHEN icd9_code BETWEEN '800' AND '99999' THEN 1
            ELSE 0
        END) AS trauma

    FROM diagnoses_icd
    GROUP BY hadm_id
)
-- =============================================================================
-- MAIN QUERY: Combine all components
-- =============================================================================
SELECT
    ie.icustay_id,
    ie.subject_id,
    ie.hadm_id,
    ie.intime AS icu_intime,
    ie.outtime AS icu_outtime,

    -- =========================================================================
    -- PRIMARY DIAGNOSIS (seq_num = 1)
    -- =========================================================================
    pd.icd9_code AS primary_icd9_code,
    dd.short_title AS primary_diagnosis_short,
    dd.long_title AS primary_diagnosis_long,

    -- =========================================================================
    -- SUMMARY STATISTICS
    -- =========================================================================
    COALESCE(dc.total_diagnoses, 0) AS total_diagnoses,
    CASE WHEN pd.icd9_code IS NOT NULL THEN 1 ELSE 0 END AS has_primary_diagnosis,

    -- =========================================================================
    -- CONDITION FLAGS (based on ALL admission diagnoses)
    -- =========================================================================
    -- Cardiovascular
    COALESCE(df.chf, 0) AS chf,
    COALESCE(df.acute_mi, 0) AS acute_mi,
    COALESCE(df.afib, 0) AS afib,
    COALESCE(df.stroke, 0) AS stroke,

    -- Respiratory
    COALESCE(df.pneumonia, 0) AS pneumonia,
    COALESCE(df.copd, 0) AS copd,
    COALESCE(df.ards, 0) AS ards,
    COALESCE(df.respiratory_failure, 0) AS respiratory_failure,

    -- Renal
    COALESCE(df.acute_renal_failure, 0) AS acute_renal_failure,
    COALESCE(df.chronic_renal_failure, 0) AS chronic_renal_failure,

    -- Infection/Other
    COALESCE(df.sepsis, 0) AS sepsis,
    COALESCE(df.diabetes, 0) AS diabetes,
    COALESCE(df.liver_disease, 0) AS liver_disease,
    COALESCE(df.malignancy, 0) AS malignancy,
    COALESCE(df.trauma, 0) AS trauma

FROM icustays ie
LEFT JOIN primary_diag pd
    ON ie.hadm_id = pd.hadm_id
LEFT JOIN d_icd_diagnoses dd
    ON pd.icd9_code = dd.icd9_code
LEFT JOIN diag_counts dc
    ON ie.hadm_id = dc.hadm_id
LEFT JOIN diag_flags df
    ON ie.hadm_id = df.hadm_id
ORDER BY ie.icustay_id;


-- ===============================================================================
-- USAGE EXAMPLES
-- ===============================================================================
-- Uncomment and run as needed

-- Example 1: View first 10 ICU stays with their primary diagnoses
-- SELECT * FROM icu_primary_diagnosis LIMIT 10;

-- Example 2: Count ICU stays by primary diagnosis
-- SELECT
--     primary_diagnosis_short,
--     COUNT(*) AS n_icu_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_primary_diagnosis
-- WHERE primary_diagnosis_short IS NOT NULL
-- GROUP BY primary_diagnosis_short
-- ORDER BY n_icu_stays DESC
-- LIMIT 20;

-- Example 3: Check data completeness
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     SUM(has_primary_diagnosis) AS stays_with_primary_diag,
--     ROUND(100.0 * SUM(has_primary_diagnosis) / COUNT(*), 2) AS pct_with_primary,
--     ROUND(AVG(total_diagnoses), 1) AS avg_diagnoses_per_stay
-- FROM icu_primary_diagnosis;

-- Example 4: Prevalence of each condition flag
-- SELECT
--     'CHF' AS condition, SUM(chf) AS n_stays,
--     ROUND(100.0 * SUM(chf) / COUNT(*), 2) AS prevalence_pct
-- FROM icu_primary_diagnosis
-- UNION ALL SELECT 'Acute MI', SUM(acute_mi), ROUND(100.0 * SUM(acute_mi) / COUNT(*), 2) FROM icu_primary_diagnosis
-- UNION ALL SELECT 'AFib', SUM(afib), ROUND(100.0 * SUM(afib) / COUNT(*), 2) FROM icu_primary_diagnosis
-- UNION ALL SELECT 'Stroke', SUM(stroke), ROUND(100.0 * SUM(stroke) / COUNT(*), 2) FROM icu_primary_diagnosis
-- UNION ALL SELECT 'Pneumonia', SUM(pneumonia), ROUND(100.0 * SUM(pneumonia) / COUNT(*), 2) FROM icu_primary_diagnosis
-- UNION ALL SELECT 'COPD', SUM(copd), ROUND(100.0 * SUM(copd) / COUNT(*), 2) FROM icu_primary_diagnosis
-- UNION ALL SELECT 'ARDS', SUM(ards), ROUND(100.0 * SUM(ards) / COUNT(*), 2) FROM icu_primary_diagnosis
-- UNION ALL SELECT 'Resp Failure', SUM(respiratory_failure), ROUND(100.0 * SUM(respiratory_failure) / COUNT(*), 2) FROM icu_primary_diagnosis
-- UNION ALL SELECT 'Acute Renal Failure', SUM(acute_renal_failure), ROUND(100.0 * SUM(acute_renal_failure) / COUNT(*), 2) FROM icu_primary_diagnosis
-- UNION ALL SELECT 'Chronic Renal Failure', SUM(chronic_renal_failure), ROUND(100.0 * SUM(chronic_renal_failure) / COUNT(*), 2) FROM icu_primary_diagnosis
-- UNION ALL SELECT 'Sepsis', SUM(sepsis), ROUND(100.0 * SUM(sepsis) / COUNT(*), 2) FROM icu_primary_diagnosis
-- UNION ALL SELECT 'Diabetes', SUM(diabetes), ROUND(100.0 * SUM(diabetes) / COUNT(*), 2) FROM icu_primary_diagnosis
-- UNION ALL SELECT 'Liver Disease', SUM(liver_disease), ROUND(100.0 * SUM(liver_disease) / COUNT(*), 2) FROM icu_primary_diagnosis
-- UNION ALL SELECT 'Malignancy', SUM(malignancy), ROUND(100.0 * SUM(malignancy) / COUNT(*), 2) FROM icu_primary_diagnosis
-- UNION ALL SELECT 'Trauma', SUM(trauma), ROUND(100.0 * SUM(trauma) / COUNT(*), 2) FROM icu_primary_diagnosis
-- ORDER BY prevalence_pct DESC;

-- Example 5: Combine with other ICU tables (e.g., icu_first_vitals)
-- SELECT
--     d.icustay_id,
--     d.primary_diagnosis_short,
--     d.sepsis,
--     d.respiratory_failure,
--     v.heartrate_first,
--     v.sysbp_first,
--     v.meanbp_first
-- FROM icu_primary_diagnosis d
-- INNER JOIN icu_first_vitals v
--     ON d.icustay_id = v.icustay_id
-- WHERE d.sepsis = 1
-- LIMIT 100;

-- Example 6: Identify ICU stays with multiple comorbidities
-- SELECT
--     icustay_id,
--     primary_diagnosis_short,
--     total_diagnoses,
--     (chf + acute_mi + afib + stroke + pneumonia + copd + ards + respiratory_failure +
--      acute_renal_failure + chronic_renal_failure + sepsis + diabetes +
--      liver_disease + malignancy + trauma) AS total_condition_flags
-- FROM icu_primary_diagnosis
-- WHERE has_primary_diagnosis = 1
-- ORDER BY total_condition_flags DESC
-- LIMIT 100;

-- Example 7: Filter ICU stays by specific condition
-- SELECT
--     icustay_id,
--     subject_id,
--     hadm_id,
--     primary_diagnosis_short,
--     total_diagnoses
-- FROM icu_primary_diagnosis
-- WHERE sepsis = 1
--   AND respiratory_failure = 1
--   AND has_primary_diagnosis = 1
-- LIMIT 100;


-- ===============================================================================
-- DIAGNOSTIC QUERIES
-- ===============================================================================
-- These queries help assess data quality and patterns

-- Query 1: Distribution of total diagnoses per ICU stay
-- SELECT
--     total_diagnoses,
--     COUNT(*) AS n_icu_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_primary_diagnosis
-- GROUP BY total_diagnoses
-- ORDER BY total_diagnoses;

-- Query 2: ICU stays without primary diagnosis
-- SELECT
--     COUNT(*) AS n_stays_without_primary,
--     ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM icu_primary_diagnosis), 2) AS pct
-- FROM icu_primary_diagnosis
-- WHERE has_primary_diagnosis = 0;

-- Query 3: Most common primary diagnoses
-- SELECT
--     primary_icd9_code,
--     primary_diagnosis_short,
--     COUNT(*) AS n_icu_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_primary_diagnosis
-- WHERE primary_icd9_code IS NOT NULL
-- GROUP BY primary_icd9_code, primary_diagnosis_short
-- ORDER BY n_icu_stays DESC
-- LIMIT 30;

-- Query 4: Co-occurrence of conditions (e.g., sepsis + respiratory failure)
-- SELECT
--     'Sepsis + Resp Failure' AS condition_combo,
--     SUM(CASE WHEN sepsis = 1 AND respiratory_failure = 1 THEN 1 ELSE 0 END) AS n_stays,
--     ROUND(100.0 * SUM(CASE WHEN sepsis = 1 AND respiratory_failure = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct
-- FROM icu_primary_diagnosis
-- UNION ALL
-- SELECT 'Sepsis + Acute Renal Failure',
--     SUM(CASE WHEN sepsis = 1 AND acute_renal_failure = 1 THEN 1 ELSE 0 END),
--     ROUND(100.0 * SUM(CASE WHEN sepsis = 1 AND acute_renal_failure = 1 THEN 1 ELSE 0 END) / COUNT(*), 2)
-- FROM icu_primary_diagnosis
-- UNION ALL
-- SELECT 'CHF + Acute Renal Failure',
--     SUM(CASE WHEN chf = 1 AND acute_renal_failure = 1 THEN 1 ELSE 0 END),
--     ROUND(100.0 * SUM(CASE WHEN chf = 1 AND acute_renal_failure = 1 THEN 1 ELSE 0 END) / COUNT(*), 2)
-- FROM icu_primary_diagnosis
-- UNION ALL
-- SELECT 'Diabetes + Chronic Renal Failure',
--     SUM(CASE WHEN diabetes = 1 AND chronic_renal_failure = 1 THEN 1 ELSE 0 END),
--     ROUND(100.0 * SUM(CASE WHEN diabetes = 1 AND chronic_renal_failure = 1 THEN 1 ELSE 0 END) / COUNT(*), 2)
-- FROM icu_primary_diagnosis;

-- Query 5: Check for multiple ICU stays from same admission
-- SELECT
--     hadm_id,
--     COUNT(DISTINCT icustay_id) AS n_icu_stays,
--     primary_diagnosis_short
-- FROM icu_primary_diagnosis
-- WHERE has_primary_diagnosis = 1
-- GROUP BY hadm_id, primary_diagnosis_short
-- HAVING COUNT(DISTINCT icustay_id) > 1
-- ORDER BY n_icu_stays DESC
-- LIMIT 20;

-- Query 6: Condition flags summary across all ICU stays
-- SELECT
--     'Total ICU Stays' AS metric, COUNT(*) AS value FROM icu_primary_diagnosis
-- UNION ALL SELECT 'Stays with Primary Diagnosis', SUM(has_primary_diagnosis) FROM icu_primary_diagnosis
-- UNION ALL SELECT 'Avg Diagnoses per Stay', CAST(ROUND(AVG(total_diagnoses), 1) AS VARCHAR) FROM icu_primary_diagnosis
-- UNION ALL SELECT 'Stays with CHF', SUM(chf) FROM icu_primary_diagnosis
-- UNION ALL SELECT 'Stays with Sepsis', SUM(sepsis) FROM icu_primary_diagnosis
-- UNION ALL SELECT 'Stays with Respiratory Failure', SUM(respiratory_failure) FROM icu_primary_diagnosis
-- UNION ALL SELECT 'Stays with Acute Renal Failure', SUM(acute_renal_failure) FROM icu_primary_diagnosis;
