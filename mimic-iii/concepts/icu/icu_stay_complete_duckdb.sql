-- =====================================================================
-- Title: Complete ICU Stay Data with Clinical Variables (DuckDB)
-- Description: Comprehensive dataset combining demographics, vitals, labs,
--              and outcomes for each ICU stay
-- MIMIC version: MIMIC-III v1.4
-- Database: DuckDB
-- =====================================================================

-- CLINICAL PURPOSE:
-- This table provides a complete clinical profile for each ICU stay by combining:
-- - Patient demographics (age, ethnicity, admission type)
-- - Readmission risk indicators
-- - First vital signs (heart rate, blood pressure, respiratory rate)
-- - First critical lab values (NT-proBNP, creatinine, BUN, potassium, cholesterol)
--
-- This is designed for:
-- - Predictive modeling (readmission, mortality, length of stay)
-- - Risk stratification
-- - Cohort selection and characterization
-- - Quality improvement analyses

-- PREREQUISITES:
-- The following tables must be created first by running their respective scripts:
--   1. icustay_detail          (from icu/icustay_detail_duckdb.sql)
--   2. icu_age                 (from icu/icu_age_duckdb.sql)
--   3. icu_readmission_30d     (from icu/icu_readmission_30d_duckdb.sql)
--   4. icu_first_hr            (from icu/icu_first_hr_duckdb.sql)
--   5. icu_first_bp            (from icu/icu_first_bp_duckdb.sql)
--   6. icu_first_resprate      (from icu/icu_first_resprate_duckdb.sql)
--   7. icu_first_ntprobnp      (from icu/icu_first_ntprobnp_duckdb.sql)
--   8. icu_first_creatinine    (from icu/icu_first_creatinine_duckdb.sql)
--   9. icu_first_bun           (from icu/icu_first_bun_duckdb.sql)
--  10. icu_first_potassium     (from icu/icu_first_potassium_duckdb.sql)
--  11. icu_first_total_cholesterol (from icu/icu_first_total_cholesterol_duckdb.sql)
--  12. admissions              (base MIMIC-III table)

-- IMPORTANT NOTES:
-- 1. "First" measurements = earliest value after ICU admission (within defined time windows)
-- 2. Time windows vary by measurement type:
--    - Vitals (HR, BP, RR): -6 hours to ICU outtime (routine monitoring)
--    - Labs (NT-proBNP, creatinine, etc.): -7 days to ICU outtime (sparse events)
-- 3. NULL values indicate measurement not available during the time window
-- 4. Age >89 is corrected to 91.4 (HIPAA de-identification)
-- 5. Readmission flags are both forward-looking (will return?) and backward-looking (is return?)

-- OUTPUT COLUMNS (per ICU stay):
--
--   IDENTIFIERS:
--   icustay_id                  : Unique ICU stay identifier
--   subject_id                  : Patient identifier
--   hadm_id                     : Hospital admission identifier
--
--   DEMOGRAPHICS:
--   age                         : Age at ICU admission (years, >89 = 91.4)
--   gender                      : Patient gender (M/F)
--   ethnicity_grouped           : Ethnicity category (white/black/hispanic/asian/native/unknown/other)
--   admission_type              : Type of hospital admission (EMERGENCY/ELECTIVE/URGENT/NEWBORN)
--
--   READMISSION FLAGS:
--   leads_to_readmission_30d    : Binary (1 = patient readmitted to ICU within 30 days after discharge)
--   is_readmission_30d          : Binary (1 = this stay is a readmission within 30 days)
--
--   VITAL SIGNS (First measurement):
--   hr_first                    : Heart rate (bpm)
--   sysbp_first                 : Systolic blood pressure (mmHg)
--   diasbp_first                : Diastolic blood pressure (mmHg)
--   resprate_first              : Respiratory rate (breaths/min)
--
--   LABORATORY VALUES (First/closest measurement):
--   ntprobnp_first              : NT-proBNP (pg/mL) - cardiac biomarker
--   creatinine_first            : Serum creatinine (mg/dL) - renal function
--   bun_first                   : Blood urea nitrogen (mg/dL) - renal function
--   potassium_first             : Serum potassium (mEq/L) - electrolyte
--   total_cholesterol_first     : Total cholesterol (mg/dL) - lipid panel
--
--   TEMPORAL INFORMATION:
--   icu_intime                  : ICU admission timestamp
--   icu_outtime                 : ICU discharge timestamp
--   los_icu                     : ICU length of stay (days)

-- =====================================================================
-- CREATE COMPREHENSIVE ICU STAY TABLE
-- =====================================================================

DROP TABLE IF EXISTS icu_stay_complete;

CREATE TABLE icu_stay_complete AS
SELECT
    -- ================================================================
    -- IDENTIFIERS
    -- ================================================================
    id.icustay_id,
    id.subject_id,
    id.hadm_id,

    -- ================================================================
    -- DEMOGRAPHICS
    -- ================================================================
    ia.age,                         -- Corrected age (>89 = 91.4)
    id.gender,
    id.ethnicity_grouped,
    adm.admission_type,

    -- ================================================================
    -- READMISSION FLAGS
    -- ================================================================
    ir.leads_to_readmission_30d,    -- Forward-looking: will this patient return?
    ir.is_readmission_30d,          -- Backward-looking: is this a readmission?

    -- ================================================================
    -- VITAL SIGNS (First measurement)
    -- ================================================================
    hr.hr_first,
    bp.sysbp_first,
    bp.diasbp_first,
    rr.resprate_first,

    -- ================================================================
    -- LABORATORY VALUES (First/closest measurement)
    -- ================================================================
    nt.ntprobnp_first,
    cr.creatinine_first,
    bun.bun_first,
    k.potassium_first,
    chol.total_cholesterol_first,

    -- ================================================================
    -- TEMPORAL INFORMATION
    -- ================================================================
    id.intime AS icu_intime,
    id.outtime AS icu_outtime,
    id.los_icu

FROM icustay_detail id

-- Join age (corrected for >89 years)
LEFT JOIN icu_age ia
    ON id.icustay_id = ia.icustay_id

-- Join readmission indicators
LEFT JOIN icu_readmission_30d ir
    ON id.icustay_id = ir.icustay_id

-- Join vital signs
LEFT JOIN icu_first_hr hr
    ON id.icustay_id = hr.icustay_id

LEFT JOIN icu_first_bp bp
    ON id.icustay_id = bp.icustay_id

LEFT JOIN icu_first_resprate rr
    ON id.icustay_id = rr.icustay_id

-- Join laboratory values
LEFT JOIN icu_first_ntprobnp nt
    ON id.icustay_id = nt.icustay_id

LEFT JOIN icu_first_creatinine cr
    ON id.icustay_id = cr.icustay_id

LEFT JOIN icu_first_bun bun
    ON id.icustay_id = bun.icustay_id

LEFT JOIN icu_first_potassium k
    ON id.icustay_id = k.icustay_id

LEFT JOIN icu_first_total_cholesterol chol
    ON id.icustay_id = chol.icustay_id

-- Join admissions table for admission_type
LEFT JOIN admissions adm
    ON id.hadm_id = adm.hadm_id

ORDER BY id.icustay_id;


-- =====================================================================
-- USAGE EXAMPLES
-- =====================================================================

-- Example 1: Basic query - all ICU stays with complete data
-- SELECT * FROM icu_stay_complete LIMIT 100;

-- Example 2: Filter for adult patients with complete vital signs
-- SELECT *
-- FROM icu_stay_complete
-- WHERE age >= 18
--     AND hr_first IS NOT NULL
--     AND sysbp_first IS NOT NULL
--     AND diasbp_first IS NOT NULL
--     AND resprate_first IS NOT NULL;

-- Example 3: Readmission analysis - exclude last ICU stays
-- SELECT
--     admission_type,
--     ethnicity_grouped,
--     COUNT(*) as total_stays,
--     SUM(leads_to_readmission_30d) as readmissions,
--     ROUND(100.0 * SUM(leads_to_readmission_30d) / COUNT(*), 2) as readmission_rate_pct
-- FROM icu_stay_complete
-- WHERE age >= 18
--     AND ir.is_last_icu_stay = 0  -- Only stays that could have readmission
-- GROUP BY admission_type, ethnicity_grouped
-- ORDER BY readmission_rate_pct DESC;

-- Example 4: Check data completeness by variable
-- SELECT
--     COUNT(*) as total_icu_stays,
--     COUNT(age) as has_age,
--     COUNT(ethnicity_grouped) as has_ethnicity,
--     COUNT(admission_type) as has_admission_type,
--     COUNT(hr_first) as has_hr,
--     COUNT(sysbp_first) as has_sbp,
--     COUNT(diasbp_first) as has_dbp,
--     COUNT(resprate_first) as has_rr,
--     COUNT(ntprobnp_first) as has_ntprobnp,
--     COUNT(creatinine_first) as has_creatinine,
--     COUNT(bun_first) as has_bun,
--     COUNT(potassium_first) as has_potassium,
--     COUNT(total_cholesterol_first) as has_cholesterol,
--     ROUND(100.0 * COUNT(hr_first) / COUNT(*), 2) as pct_has_hr,
--     ROUND(100.0 * COUNT(ntprobnp_first) / COUNT(*), 2) as pct_has_ntprobnp,
--     ROUND(100.0 * COUNT(total_cholesterol_first) / COUNT(*), 2) as pct_has_cholesterol
-- FROM icu_stay_complete;

-- Example 5: Export to CSV for analysis
-- COPY (
--     SELECT * FROM icu_stay_complete
--     WHERE age >= 18  -- Adults only
-- ) TO 'icu_stay_complete_adults.csv' WITH (HEADER, DELIMITER ',');

-- Example 6: Create a filtered cohort for modeling
-- CREATE TABLE icu_modeling_cohort AS
-- SELECT *
-- FROM icu_stay_complete
-- WHERE age >= 18                     -- Adults
--     AND age < 90                    -- Exclude de-identified elderly
--     AND hr_first IS NOT NULL        -- Has vital signs
--     AND sysbp_first IS NOT NULL
--     AND diasbp_first IS NOT NULL
--     AND resprate_first IS NOT NULL
--     AND creatinine_first IS NOT NULL  -- Has key lab values
--     AND bun_first IS NOT NULL
--     AND potassium_first IS NOT NULL;

-- Example 7: Summary statistics by admission type
-- SELECT
--     admission_type,
--     COUNT(*) as n_stays,
--     ROUND(AVG(age), 1) as mean_age,
--     ROUND(AVG(hr_first), 1) as mean_hr,
--     ROUND(AVG(sysbp_first), 1) as mean_sbp,
--     ROUND(AVG(creatinine_first), 2) as mean_creatinine,
--     ROUND(100.0 * SUM(leads_to_readmission_30d) / NULLIF(COUNT(*), 0), 2) as readmission_rate_pct
-- FROM icu_stay_complete
-- WHERE age >= 18
-- GROUP BY admission_type
-- ORDER BY n_stays DESC;

-- Example 8: Identify patients with abnormal values
-- SELECT
--     icustay_id,
--     subject_id,
--     age,
--     ethnicity_grouped,
--     hr_first,
--     sysbp_first,
--     creatinine_first,
--     potassium_first,
--     CASE
--         WHEN hr_first < 60 THEN 'Bradycardia'
--         WHEN hr_first > 100 THEN 'Tachycardia'
--         ELSE 'Normal HR'
--     END as hr_category,
--     CASE
--         WHEN sysbp_first < 90 THEN 'Hypotensive'
--         WHEN sysbp_first > 140 THEN 'Hypertensive'
--         ELSE 'Normal BP'
--     END as bp_category,
--     CASE
--         WHEN creatinine_first > 1.5 THEN 'Elevated creatinine'
--         ELSE 'Normal creatinine'
--     END as renal_status
-- FROM icu_stay_complete
-- WHERE age >= 18
--     AND hr_first IS NOT NULL
--     AND sysbp_first IS NOT NULL
--     AND creatinine_first IS NOT NULL
-- LIMIT 100;


-- =====================================================================
-- DIAGNOSTIC QUERIES - Data Quality Assessment
-- =====================================================================

-- Query 1: Overall statistics
-- SELECT
--     COUNT(*) as total_icu_stays,
--     COUNT(DISTINCT subject_id) as unique_patients,
--     COUNT(DISTINCT hadm_id) as unique_admissions,
--     ROUND(AVG(age), 1) as mean_age,
--     ROUND(AVG(los_icu), 2) as mean_los_days
-- FROM icu_stay_complete;

-- Query 2: Missing data patterns
-- SELECT
--     'All stays' as cohort,
--     COUNT(*) as n,
--     ROUND(100.0 * COUNT(hr_first) / COUNT(*), 1) as pct_hr,
--     ROUND(100.0 * COUNT(sysbp_first) / COUNT(*), 1) as pct_sbp,
--     ROUND(100.0 * COUNT(ntprobnp_first) / COUNT(*), 1) as pct_ntprobnp,
--     ROUND(100.0 * COUNT(creatinine_first) / COUNT(*), 1) as pct_creatinine,
--     ROUND(100.0 * COUNT(bun_first) / COUNT(*), 1) as pct_bun,
--     ROUND(100.0 * COUNT(potassium_first) / COUNT(*), 1) as pct_potassium,
--     ROUND(100.0 * COUNT(total_cholesterol_first) / COUNT(*), 1) as pct_cholesterol
-- FROM icu_stay_complete;

-- Query 3: Distribution by admission type
-- SELECT
--     admission_type,
--     COUNT(*) as n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct_stays
-- FROM icu_stay_complete
-- GROUP BY admission_type
-- ORDER BY n_stays DESC;

-- Query 4: Distribution by ethnicity
-- SELECT
--     ethnicity_grouped,
--     COUNT(*) as n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct_stays
-- FROM icu_stay_complete
-- GROUP BY ethnicity_grouped
-- ORDER BY n_stays DESC;

-- Query 5: Vital signs ranges (detect outliers)
-- SELECT
--     'Heart Rate' as variable,
--     MIN(hr_first) as min_val,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY hr_first) as p25,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY hr_first) as median,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY hr_first) as p75,
--     MAX(hr_first) as max_val
-- FROM icu_stay_complete
-- WHERE hr_first IS NOT NULL
-- UNION ALL
-- SELECT
--     'Systolic BP' as variable,
--     MIN(sysbp_first),
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY sysbp_first),
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY sysbp_first),
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY sysbp_first),
--     MAX(sysbp_first)
-- FROM icu_stay_complete
-- WHERE sysbp_first IS NOT NULL
-- UNION ALL
-- SELECT
--     'Respiratory Rate' as variable,
--     MIN(resprate_first),
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY resprate_first),
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY resprate_first),
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY resprate_first),
--     MAX(resprate_first)
-- FROM icu_stay_complete
-- WHERE resprate_first IS NOT NULL;

-- Query 6: Lab value ranges
-- SELECT
--     'Creatinine (mg/dL)' as variable,
--     MIN(creatinine_first) as min_val,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY creatinine_first) as p25,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY creatinine_first) as median,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY creatinine_first) as p75,
--     MAX(creatinine_first) as max_val
-- FROM icu_stay_complete
-- WHERE creatinine_first IS NOT NULL
-- UNION ALL
-- SELECT
--     'BUN (mg/dL)',
--     MIN(bun_first),
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY bun_first),
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY bun_first),
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY bun_first),
--     MAX(bun_first)
-- FROM icu_stay_complete
-- WHERE bun_first IS NOT NULL
-- UNION ALL
-- SELECT
--     'Potassium (mEq/L)',
--     MIN(potassium_first),
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY potassium_first),
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY potassium_first),
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY potassium_first),
--     MAX(potassium_first)
-- FROM icu_stay_complete
-- WHERE potassium_first IS NOT NULL;
