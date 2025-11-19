-- =====================================================================
-- Title: Detailed ICU Stay Information (DuckDB)
-- Description: Comprehensive demographic and administrative information
--              for ICU stays combining patients, admissions, and ICU data
-- MIMIC version: MIMIC-III v1.4
-- Database: DuckDB
-- =====================================================================

-- CLINICAL PURPOSE:
-- Provides a complete demographic and administrative profile for each ICU stay:
-- - Patient demographics (age, gender, ethnicity, mortality)
-- - Hospital admission details (dates, length of stay, expire flag)
-- - ICU stay timing and sequencing
-- - First stay indicators for both hospital and ICU

-- IMPORTANT NOTES:
-- 1. Age calculation follows HIPAA de-identification (>89 years shifted)
-- 2. Ethnicity is provided both raw and grouped into 6 categories
-- 3. Sequence numbers track multiple admissions/ICU stays per patient
-- 4. hospital_expire_flag indicates death during THIS hospital admission
-- 5. Creates a materialized TABLE for better performance and reusability

-- OUTPUT COLUMNS:
--   IDENTIFIERS:
--   icustay_id              : Primary key - unique ICU stay identifier
--   subject_id              : Patient identifier (may appear multiple times)
--   hadm_id                 : Hospital admission identifier
--
--   PATIENT DEMOGRAPHICS:
--   gender                  : Patient gender (M/F)
--   dod                     : Date of death (if applicable, NULL if alive)
--   admission_age           : Age at ICU admission in years (>89 will be very large due to date shifting)
--
--   HOSPITAL ADMISSION:
--   admittime               : Hospital admission timestamp
--   dischtime               : Hospital discharge timestamp
--   los_hospital            : Hospital length of stay in days (fractional)
--   ethnicity               : Raw ethnicity string from admissions table
--   ethnicity_grouped       : Grouped ethnicity (white/black/hispanic/asian/native/unknown/other)
--   hospital_expire_flag    : Binary flag (1 = died during hospital stay, 0 = survived to discharge)
--   hospstay_seq            : Hospital admission sequence number per patient (1 = first admission)
--   first_hosp_stay         : Binary flag (1 = first hospital admission for patient, 0 = subsequent)
--
--   ICU STAY:
--   intime                  : ICU admission timestamp
--   outtime                 : ICU discharge timestamp
--   los_icu                 : ICU length of stay in days (fractional)
--   icustay_seq             : ICU stay sequence number per hospital admission (1 = first ICU stay)
--   first_icu_stay          : Binary flag (1 = first ICU stay of this hospitalization, 0 = subsequent)

-- =====================================================================
-- CREATE MATERIALIZED TABLE (Recommended)
-- =====================================================================
-- This creates a permanent table that can be easily joined with other tables
-- Use this for production queries and analyses

DROP TABLE IF EXISTS icustay_detail;
CREATE TABLE icustay_detail AS
SELECT
    ie.icustay_id,
    ie.subject_id,
    ie.hadm_id,

    -- ================================================================
    -- PATIENT-LEVEL FACTORS
    -- ================================================================
    pat.gender,
    pat.dod,

    -- ================================================================
    -- HOSPITAL-LEVEL FACTORS
    -- ================================================================
    adm.admittime,
    adm.dischtime,

    -- Hospital length of stay in days
    DATE_DIFF('day', adm.admittime, adm.dischtime) AS los_hospital,

    -- Age at ICU admission in years
    -- Note: Patients >89 have DOB shifted ~300 years into past for HIPAA
    -- This will result in ages >200 for elderly patients
    DATE_DIFF('year', pat.dob, ie.intime) AS admission_age,

    -- Raw ethnicity
    adm.ethnicity,

    -- Grouped ethnicity categories
    CASE
        WHEN adm.ethnicity IN (
            'WHITE',
            'WHITE - RUSSIAN',
            'WHITE - OTHER EUROPEAN',
            'WHITE - BRAZILIAN',
            'WHITE - EASTERN EUROPEAN'
        ) THEN 'white'

        WHEN adm.ethnicity IN (
            'BLACK/AFRICAN AMERICAN',
            'BLACK/CAPE VERDEAN',
            'BLACK/HAITIAN',
            'BLACK/AFRICAN',
            'CARIBBEAN ISLAND'
        ) THEN 'black'

        WHEN adm.ethnicity IN (
            'HISPANIC OR LATINO',
            'HISPANIC/LATINO - PUERTO RICAN',
            'HISPANIC/LATINO - DOMINICAN',
            'HISPANIC/LATINO - GUATEMALAN',
            'HISPANIC/LATINO - CUBAN',
            'HISPANIC/LATINO - SALVADORAN',
            'HISPANIC/LATINO - CENTRAL AMERICAN (OTHER)',
            'HISPANIC/LATINO - MEXICAN',
            'HISPANIC/LATINO - COLOMBIAN',
            'HISPANIC/LATINO - HONDURAN'
        ) THEN 'hispanic'

        WHEN adm.ethnicity IN (
            'ASIAN',
            'ASIAN - CHINESE',
            'ASIAN - ASIAN INDIAN',
            'ASIAN - VIETNAMESE',
            'ASIAN - FILIPINO',
            'ASIAN - CAMBODIAN',
            'ASIAN - OTHER',
            'ASIAN - KOREAN',
            'ASIAN - JAPANESE',
            'ASIAN - THAI'
        ) THEN 'asian'

        WHEN adm.ethnicity IN (
            'AMERICAN INDIAN/ALASKA NATIVE',
            'AMERICAN INDIAN/ALASKA NATIVE FEDERALLY RECOGNIZED TRIBE'
        ) THEN 'native'

        WHEN adm.ethnicity IN (
            'UNKNOWN/NOT SPECIFIED',
            'UNABLE TO OBTAIN',
            'PATIENT DECLINED TO ANSWER'
        ) THEN 'unknown'

        -- Includes: OTHER, MULTI RACE ETHNICITY, PORTUGUESE, MIDDLE EASTERN,
        -- NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER, SOUTH AMERICAN
        ELSE 'other'
    END AS ethnicity_grouped,

    -- Hospital mortality flag (1 = died during this hospitalization, 0 = survived)
    adm.hospital_expire_flag,

    -- Hospital stay sequence number (1 = first admission, 2 = second, etc.)
    DENSE_RANK() OVER (PARTITION BY adm.subject_id ORDER BY adm.admittime) AS hospstay_seq,

    -- Flag: is this the patient's first hospital admission?
    CASE
        WHEN DENSE_RANK() OVER (PARTITION BY adm.subject_id ORDER BY adm.admittime) = 1
        THEN 1
        ELSE 0
    END AS first_hosp_stay,

    -- ================================================================
    -- ICU-LEVEL FACTORS
    -- ================================================================
    ie.intime,
    ie.outtime,

    -- ICU length of stay in days
    DATE_DIFF('day', ie.intime, ie.outtime) AS los_icu,

    -- ICU stay sequence number within this hospital admission (1 = first ICU stay of this hospitalization)
    DENSE_RANK() OVER (PARTITION BY ie.hadm_id ORDER BY ie.intime) AS icustay_seq,

    -- Flag: is this the first ICU stay for this hospital admission?
    CASE
        WHEN DENSE_RANK() OVER (PARTITION BY ie.hadm_id ORDER BY ie.intime) = 1
        THEN 1
        ELSE 0
    END AS first_icu_stay

FROM icustays ie
INNER JOIN admissions adm
    ON ie.hadm_id = adm.hadm_id
INNER JOIN patients pat
    ON ie.subject_id = pat.subject_id
WHERE adm.has_chartevents_data = 1
ORDER BY ie.subject_id, adm.admittime, ie.intime;


-- =====================================================================
-- ALTERNATIVE: CREATE VIEW (Lightweight, always up-to-date)
-- =====================================================================
-- Uncomment below if you prefer a VIEW instead of a materialized TABLE
-- VIEWs are lighter but recalculate on every query
-- TABLEs are faster for repeated queries but need to be refreshed if base data changes

-- DROP VIEW IF EXISTS icustay_detail;
-- CREATE VIEW icustay_detail AS
-- SELECT ... (same query as above)


-- =====================================================================
-- USAGE EXAMPLES - Using the icustay_detail table
-- =====================================================================

-- Example 1: Simple query - all ICU stays with demographics
-- SELECT * FROM icustay_detail LIMIT 100;

-- Example 2: Filter for first hospital admissions only
-- SELECT * FROM icustay_detail
-- WHERE first_hosp_stay = 1;

-- Example 3: Filter for first ICU stays within each hospitalization
-- SELECT * FROM icustay_detail
-- WHERE first_icu_stay = 1;

-- Example 4: Exclude patients who died in hospital
-- SELECT * FROM icustay_detail
-- WHERE hospital_expire_flag = 0;

-- Example 5: Adult patients (>= 16 years) only
-- Note: admission_age > 89 will show very large values (200+) due to HIPAA date shifting
-- SELECT * FROM icustay_detail
-- WHERE admission_age >= 16 AND admission_age < 200;

-- Example 6: Ethnic distribution of ICU patients
-- SELECT
--     ethnicity_grouped,
--     COUNT(*) as n_stays,
--     COUNT(DISTINCT subject_id) as n_patients,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct_stays
-- FROM icustay_detail
-- GROUP BY ethnicity_grouped
-- ORDER BY n_stays DESC;

-- Example 7: In-hospital mortality by ethnicity
-- SELECT
--     ethnicity_grouped,
--     COUNT(*) as total_stays,
--     SUM(hospital_expire_flag) as deaths,
--     ROUND(100.0 * SUM(hospital_expire_flag) / COUNT(*), 2) as mortality_rate_pct
-- FROM icustay_detail
-- WHERE admission_age >= 16 AND admission_age < 200
-- GROUP BY ethnicity_grouped
-- ORDER BY mortality_rate_pct DESC;

-- Example 8: Patients with multiple hospital admissions
-- SELECT
--     subject_id,
--     MAX(hospstay_seq) as total_admissions,
--     COUNT(*) as total_icu_stays
-- FROM icustay_detail
-- GROUP BY subject_id
-- HAVING MAX(hospstay_seq) > 1
-- ORDER BY total_admissions DESC;

-- Example 9: Patients with multiple ICU stays within single hospitalization
-- SELECT
--     hadm_id,
--     subject_id,
--     MAX(icustay_seq) as icu_stays_per_hospitalization
-- FROM icustay_detail
-- GROUP BY hadm_id, subject_id
-- HAVING MAX(icustay_seq) > 1
-- ORDER BY icu_stays_per_hospitalization DESC;

-- Example 10: Join with icu_age for corrected age (replacing >89 with 91.4)
-- SELECT
--     id.icustay_id,
--     id.subject_id,
--     id.gender,
--     id.admission_age as age_raw,
--     ia.age as age_corrected,
--     id.ethnicity_grouped,
--     id.los_hospital,
--     id.los_icu,
--     id.hospital_expire_flag,
--     id.first_hosp_stay,
--     id.first_icu_stay
-- FROM icustay_detail id
-- INNER JOIN icu_age ia
--     ON id.icustay_id = ia.icustay_id
-- WHERE ia.age >= 16;

-- Example 11: Join with icu_readmission_30d for combined demographics + readmission
-- SELECT
--     id.icustay_id,
--     id.subject_id,
--     id.gender,
--     id.admission_age,
--     id.ethnicity_grouped,
--     id.los_icu,
--     id.hospital_expire_flag,
--     ir.leads_to_readmission_30d,
--     ir.days_to_next_icu
-- FROM icustay_detail id
-- INNER JOIN icu_readmission_30d ir
--     ON id.icustay_id = ir.icustay_id
-- WHERE id.first_icu_stay = 1  -- First ICU stay per hospitalization
--     AND id.admission_age >= 16
--     AND id.admission_age < 200;

-- Example 12: Export to CSV
-- COPY (SELECT * FROM icustay_detail WHERE admission_age >= 16 AND admission_age < 200)
-- TO 'icustay_detail_adults.csv' WITH (HEADER, DELIMITER ',');

-- Example 13: Create filtered cohort table
-- CREATE TABLE icu_cohort_first_stays AS
-- SELECT * FROM icustay_detail
-- WHERE first_hosp_stay = 1
--     AND first_icu_stay = 1
--     AND admission_age >= 16
--     AND admission_age < 200;


-- =====================================================================
-- DIAGNOSTIC QUERIES - Understanding your data
-- =====================================================================

-- Query 1: Overall statistics
-- SELECT
--     COUNT(*) as total_icu_stays,
--     COUNT(DISTINCT subject_id) as unique_patients,
--     COUNT(DISTINCT hadm_id) as unique_hospitalizations,
--     ROUND(AVG(los_hospital), 2) as mean_hospital_los_days,
--     ROUND(AVG(los_icu), 2) as mean_icu_los_days,
--     SUM(hospital_expire_flag) as hospital_deaths,
--     ROUND(100.0 * SUM(hospital_expire_flag) / COUNT(*), 2) as mortality_rate_pct
-- FROM icustay_detail;

-- Query 2: Age distribution (handling de-identified elderly)
-- SELECT
--     CASE
--         WHEN admission_age < 18 THEN 'Pediatric (<18)'
--         WHEN admission_age < 40 THEN 'Young Adult (18-39)'
--         WHEN admission_age < 65 THEN 'Middle Age (40-64)'
--         WHEN admission_age < 90 THEN 'Senior (65-89)'
--         ELSE 'Elderly (>89, de-identified)'
--     END AS age_category,
--     COUNT(*) as n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct_stays
-- FROM icustay_detail
-- GROUP BY age_category
-- ORDER BY
--     CASE
--         WHEN admission_age < 18 THEN 1
--         WHEN admission_age < 40 THEN 2
--         WHEN admission_age < 65 THEN 3
--         WHEN admission_age < 90 THEN 4
--         ELSE 5
--     END;

-- Query 3: Gender distribution
-- SELECT
--     gender,
--     COUNT(*) as n_stays,
--     COUNT(DISTINCT subject_id) as n_patients,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct_stays
-- FROM icustay_detail
-- GROUP BY gender;

-- Query 4: Ethnicity distribution (grouped)
-- SELECT
--     ethnicity_grouped,
--     COUNT(*) as n_stays,
--     COUNT(DISTINCT subject_id) as n_patients,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct_stays
-- FROM icustay_detail
-- GROUP BY ethnicity_grouped
-- ORDER BY n_stays DESC;

-- Query 5: First vs subsequent hospital admissions
-- SELECT
--     first_hosp_stay,
--     COUNT(*) as n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct_stays
-- FROM icustay_detail
-- GROUP BY first_hosp_stay;

-- Query 6: First vs subsequent ICU stays (within same hospitalization)
-- SELECT
--     first_icu_stay,
--     COUNT(*) as n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct_stays
-- FROM icustay_detail
-- GROUP BY first_icu_stay;

-- Query 7: Hospital mortality rate by demographic factors
-- SELECT
--     CASE
--         WHEN admission_age < 40 THEN '18-39'
--         WHEN admission_age < 65 THEN '40-64'
--         WHEN admission_age < 90 THEN '65-89'
--         ELSE '>89'
--     END AS age_category,
--     gender,
--     ethnicity_grouped,
--     COUNT(*) as n_stays,
--     SUM(hospital_expire_flag) as deaths,
--     ROUND(100.0 * SUM(hospital_expire_flag) / COUNT(*), 2) as mortality_rate_pct
-- FROM icustay_detail
-- WHERE admission_age >= 16
-- GROUP BY age_category, gender, ethnicity_grouped
-- ORDER BY mortality_rate_pct DESC;

-- Query 8: Length of stay distributions
-- SELECT
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY los_hospital) as hospital_los_p25,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY los_hospital) as hospital_los_median,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY los_hospital) as hospital_los_p75,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY los_icu) as icu_los_p25,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY los_icu) as icu_los_median,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY los_icu) as icu_los_p75
-- FROM icustay_detail;

-- Query 9: Patients with multiple hospitalizations
-- SELECT
--     MAX(hospstay_seq) as hospitalization_count,
--     COUNT(DISTINCT subject_id) as n_patients
-- FROM icustay_detail
-- GROUP BY subject_id, MAX(hospstay_seq)
-- ORDER BY hospitalization_count;

-- Query 10: ICU stays per hospitalization distribution
-- SELECT
--     MAX(icustay_seq) as icu_stays_per_hosp,
--     COUNT(*) as n_hospitalizations
-- FROM icustay_detail
-- GROUP BY hadm_id
-- HAVING MAX(icustay_seq) >= 1
-- ORDER BY icu_stays_per_hosp;
