-- =====================================================================
-- Title: ICU Admission Age Calculation (DuckDB)
-- Description: Calculates patient age at ICU admission for MIMIC-III
--              Unit of analysis: ICU stays (icustay_id)
--              Handles HIPAA de-identification for patients >89 years
-- MIMIC version: MIMIC-III v1.4
-- Database: DuckDB
-- =====================================================================

-- IMPORTANT NOTES:
-- 1. Patients >89 years have DOB shifted ~300 years into the past for privacy
-- 2. Ages >89 are replaced with 91.4 (median age of elderly cohort)
-- 3. This query includes ALL ICU stays (patients may have multiple)
-- 4. No filters applied by default - customize as needed
-- 5. Creates a materialized TABLE for better performance and reusability

-- OUTPUT COLUMNS:
--   icustay_id         : Primary key - unique ICU stay identifier
--   subject_id         : Patient identifier (may appear multiple times)
--   hadm_id            : Hospital admission identifier
--   icu_intime         : ICU admission timestamp
--   icu_outtime        : ICU discharge timestamp
--   dob                : Date of birth (shifted for patients >89)
--   gender             : Patient gender (M/F)
--   age_raw            : Raw calculated age (may be >200 for elderly)
--   age                : Corrected age (>89 replaced with 91.4)
--   icu_los_days       : ICU length of stay in days
--   icustay_num        : ICU stay sequence number per patient
--   icustay_num_hosp   : ICU stay sequence number per hospital admission

-- =====================================================================
-- CREATE MATERIALIZED TABLE (Recommended)
-- =====================================================================
-- This creates a permanent table that can be easily joined with other tables
-- Use this for production queries and analyses

DROP TABLE IF EXISTS icu_age;
CREATE TABLE icu_age AS
SELECT
    ie.icustay_id,
    ie.subject_id,
    ie.hadm_id,
    ie.intime AS icu_intime,
    ie.outtime AS icu_outtime,
    pat.dob,
    pat.gender,

    -- Raw age calculation: (intime - dob) converted to years
    -- Formula: seconds / (365.242 days/year * 24 hours/day * 60 min/hour * 60 sec/min)
    DATE_DIFF('second', pat.dob, ie.intime) / 365.242 / 24.0 / 60.0 / 60.0 AS age_raw,

    -- Corrected age: Replace >89 with 91.4 (median of de-identified elderly)
    CASE
        WHEN DATE_DIFF('second', pat.dob, ie.intime) / 365.242 / 24.0 / 60.0 / 60.0 > 89
        THEN 91.4
        ELSE DATE_DIFF('second', pat.dob, ie.intime) / 365.242 / 24.0 / 60.0 / 60.0
    END AS age,

    -- ICU length of stay in days (fractional)
    DATE_DIFF('second', ie.intime, ie.outtime) / 24.0 / 60.0 / 60.0 AS icu_los_days,

    -- ICU stay sequence number for this patient (1 = first ICU stay)
    ROW_NUMBER() OVER (PARTITION BY ie.subject_id ORDER BY ie.intime) AS icustay_num,

    -- ICU stay sequence number for this hospital admission (1 = first ICU stay of this hospitalization)
    ROW_NUMBER() OVER (PARTITION BY ie.hadm_id ORDER BY ie.intime) AS icustay_num_hosp

FROM icustays ie
INNER JOIN patients pat
    ON ie.subject_id = pat.subject_id
ORDER BY icustay_id;


-- =====================================================================
-- ALTERNATIVE: CREATE VIEW (Lightweight, always up-to-date)
-- =====================================================================
-- Uncomment below if you prefer a VIEW instead of a materialized TABLE
-- VIEWs are lighter but recalculate on every query
-- TABLEs are faster for repeated queries but need to be refreshed if base data changes

-- DROP VIEW IF EXISTS icu_age;
-- CREATE VIEW icu_age AS
-- SELECT ... (same query as above)


-- =====================================================================
-- USAGE EXAMPLES - Using the icu_age table
-- =====================================================================

-- Example 1: Simple query - all ICU stays
-- SELECT * FROM icu_age;

-- Example 2: Filter for adults only (>= 16 years)
-- SELECT * FROM icu_age WHERE age_raw >= 16;

-- Example 3: First ICU stay per patient, adults only
-- SELECT * FROM icu_age
-- WHERE age_raw >= 16 AND icustay_num = 1;

-- Example 4: Adults with ICU stay >= 24 hours
-- SELECT * FROM icu_age
-- WHERE age_raw >= 16 AND icu_los_days >= 1.0;

-- Example 5: Join with admissions for hospital-level details
-- SELECT
--     ia.icustay_id,
--     ia.subject_id,
--     ia.age,
--     ia.gender,
--     ia.icu_los_days,
--     adm.admission_type,
--     adm.ethnicity,
--     adm.hospital_expire_flag
-- FROM icu_age ia
-- INNER JOIN admissions adm
--     ON ia.hadm_id = adm.hadm_id
-- WHERE ia.age_raw >= 16;

-- Example 6: Join with chartevents to get vital signs
-- SELECT
--     ia.icustay_id,
--     ia.age,
--     ce.itemid,
--     ce.charttime,
--     ce.valuenum
-- FROM icu_age ia
-- INNER JOIN chartevents ce
--     ON ia.icustay_id = ce.icustay_id
-- WHERE ia.age_raw >= 16
--     AND ce.itemid IN (211, 220045)  -- Heart rate
-- LIMIT 100;

-- Example 7: Add age categories
-- SELECT
--     *,
--     CASE
--         WHEN age < 18 THEN 'Pediatric (<18)'
--         WHEN age < 40 THEN 'Young Adult (18-39)'
--         WHEN age < 65 THEN 'Middle Age (40-64)'
--         WHEN age < 90 THEN 'Senior (65-89)'
--         ELSE 'Elderly (90+)'
--     END AS age_category
-- FROM icu_age;

-- Example 8: Export to CSV
-- COPY (SELECT * FROM icu_age WHERE age_raw >= 16)
-- TO 'icu_ages.csv' WITH (HEADER, DELIMITER ',');

-- Example 9: Create a filtered cohort table
-- CREATE TABLE icu_cohort_adults AS
-- SELECT * FROM icu_age WHERE age_raw >= 16;


-- =====================================================================
-- DIAGNOSTIC QUERIES - Understanding your data
-- =====================================================================

-- Query 1: Overall statistics
-- SELECT
--     COUNT(*) as total_icu_stays,
--     COUNT(DISTINCT subject_id) as unique_patients,
--     ROUND(AVG(age), 1) as mean_age,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY age), 1) as median_age,
--     ROUND(MIN(age), 1) as min_age,
--     ROUND(MAX(age), 1) as max_age,
--     SUM(CASE WHEN age_raw > 89 THEN 1 ELSE 0 END) as elderly_deidentified_count,
--     ROUND(100.0 * SUM(CASE WHEN age_raw > 89 THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_elderly
-- FROM icu_age;

-- Query 2: Age distribution by gender
-- SELECT
--     gender,
--     COUNT(*) as n_stays,
--     ROUND(AVG(age), 1) as mean_age,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY age), 1) as median_age
-- FROM icu_age
-- GROUP BY gender;

-- Query 3: Check for multiple ICU stays
-- SELECT
--     icustay_num,
--     COUNT(*) as n_stays
-- FROM icu_age
-- GROUP BY icustay_num
-- ORDER BY icustay_num;

-- Query 4: ICU length of stay by age category
-- SELECT
--     CASE
--         WHEN age < 40 THEN '18-39'
--         WHEN age < 65 THEN '40-64'
--         WHEN age < 90 THEN '65-89'
--         ELSE '90+'
--     END AS age_category,
--     COUNT(*) as n_stays,
--     ROUND(AVG(icu_los_days), 2) as mean_los_days,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY icu_los_days), 2) as median_los_days
-- FROM icu_age
-- WHERE age_raw >= 16
-- GROUP BY age_category
-- ORDER BY age_category;
