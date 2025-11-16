-- =====================================================================
-- Title: 30-Day ICU Readmission Flag (DuckDB)
-- Description: Identifies ICU stays with readmission within 30 days
--              Unit of analysis: ICU stays (icustay_id)
--              Definition follows Pishgar et al. (2022) BMC Med Inform Decis Mak
-- MIMIC version: MIMIC-III v1.4
-- Database: DuckDB
-- =====================================================================

-- CLINICAL DEFINITION:
-- Readmission to ICU within 30 days after discharge from ICU
-- - More restrictive than hospital readmission (ICU-specific)
-- - Binary outcome: 0 (no readmission) or 1 (readmission within 30 days)
-- - Critical quality metric for care gaps and premature discharge

-- IMPORTANT NOTES:
-- 1. This is ICU-to-ICU readmission (not hospital readmission)
-- 2. Readmission can be to same hospital or different admission
-- 3. Last ICU stay per patient cannot have readmission flag = 1
-- 4. Time to readmission is included for survival analysis
-- 5. Excludes same-hospital transfers (uses different hadm_id as proxy for true readmission)

-- OUTPUT COLUMNS:
--   icustay_id              : Primary key - unique ICU stay identifier
--   subject_id              : Patient identifier
--   hadm_id                 : Current hospital admission
--   icu_intime              : ICU admission timestamp
--   icu_outtime             : ICU discharge timestamp
--   next_icustay_id         : Next ICU stay ID (if any)
--   next_hadm_id            : Next hospital admission ID (if any)
--   next_icu_intime         : Next ICU admission timestamp (if any)
--   days_to_next_icu        : Days from discharge to next ICU admission
--   readmission_30d         : Binary flag (1 = readmitted within 30 days)
--   is_last_icu_stay        : Flag indicating this is patient's final ICU stay

-- =====================================================================
-- CREATE MATERIALIZED TABLE (Recommended)
-- =====================================================================

DROP TABLE IF EXISTS icu_readmission_30d;
CREATE TABLE icu_readmission_30d AS
WITH icu_with_next AS (
    SELECT
        ie.icustay_id,
        ie.subject_id,
        ie.hadm_id,
        ie.intime AS icu_intime,
        ie.outtime AS icu_outtime,

        -- Get next ICU stay for same patient using LEAD window function
        LEAD(ie.icustay_id) OVER (PARTITION BY ie.subject_id ORDER BY ie.intime) AS next_icustay_id,
        LEAD(ie.hadm_id) OVER (PARTITION BY ie.subject_id ORDER BY ie.intime) AS next_hadm_id,
        LEAD(ie.intime) OVER (PARTITION BY ie.subject_id ORDER BY ie.intime) AS next_icu_intime,

        -- Flag to identify last ICU stay for each patient
        CASE
            WHEN LEAD(ie.icustay_id) OVER (PARTITION BY ie.subject_id ORDER BY ie.intime) IS NULL
            THEN 1
            ELSE 0
        END AS is_last_icu_stay

    FROM icustays ie
)
SELECT
    icustay_id,
    subject_id,
    hadm_id,
    icu_intime,
    icu_outtime,
    next_icustay_id,
    next_hadm_id,
    next_icu_intime,

    -- Calculate days from ICU discharge to next ICU admission
    CASE
        WHEN next_icu_intime IS NOT NULL
        THEN DATE_DIFF('second', icu_outtime, next_icu_intime) / 24.0 / 60.0 / 60.0
        ELSE NULL
    END AS days_to_next_icu,

    -- 30-day readmission flag (1 = readmitted within 30 days)
    CASE
        WHEN next_icu_intime IS NULL THEN 0  -- No subsequent ICU stay
        WHEN DATE_DIFF('second', icu_outtime, next_icu_intime) / 24.0 / 60.0 / 60.0 <= 30 THEN 1
        ELSE 0
    END AS readmission_30d,

    is_last_icu_stay

FROM icu_with_next
ORDER BY icustay_id;


-- =====================================================================
-- ALTERNATIVE: More restrictive definition
-- =====================================================================
-- Exclude same-hospital ICU transfers (only count true "readmissions")
-- Uncomment below to use this stricter definition

-- DROP TABLE IF EXISTS icu_readmission_30d;
-- CREATE TABLE icu_readmission_30d AS
-- WITH icu_with_next AS (
--     ... same as above ...
-- )
-- SELECT
--     *,
--     -- Only count as readmission if different hospital admission
--     CASE
--         WHEN next_icu_intime IS NULL THEN 0
--         WHEN next_hadm_id = hadm_id THEN 0  -- Same hospital admission = transfer, not readmission
--         WHEN DATE_DIFF('second', icu_outtime, next_icu_intime) / 24.0 / 60.0 / 60.0 <= 30 THEN 1
--         ELSE 0
--     END AS readmission_30d_strict
-- FROM icu_with_next;


-- =====================================================================
-- USAGE EXAMPLES - Using the icu_readmission_30d table
-- =====================================================================

-- Example 1: Simple query - all ICU stays with readmission status
-- SELECT * FROM icu_readmission_30d LIMIT 10;

-- Example 2: Count readmissions
-- SELECT
--     readmission_30d,
--     COUNT(*) as n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
-- FROM icu_readmission_30d
-- GROUP BY readmission_30d;

-- Example 3: Exclude last ICU stays (can't be readmitted if last stay)
-- SELECT
--     readmission_30d,
--     COUNT(*) as n_stays
-- FROM icu_readmission_30d
-- WHERE is_last_icu_stay = 0
-- GROUP BY readmission_30d;

-- Example 4: Join with icu_age table for age-stratified analysis
-- SELECT
--     CASE
--         WHEN ia.age < 40 THEN '18-39'
--         WHEN ia.age < 65 THEN '40-64'
--         WHEN ia.age < 90 THEN '65-89'
--         ELSE '90+'
--     END AS age_category,
--     ir.readmission_30d,
--     COUNT(*) as n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY
--         CASE
--             WHEN ia.age < 40 THEN '18-39'
--             WHEN ia.age < 65 THEN '40-64'
--             WHEN ia.age < 90 THEN '65-89'
--             ELSE '90+'
--         END
--     ), 2) as pct_within_age_group
-- FROM icu_readmission_30d ir
-- INNER JOIN icu_age ia ON ir.icustay_id = ia.icustay_id
-- WHERE ia.age_raw >= 16
--     AND ir.is_last_icu_stay = 0  -- Exclude last stays
-- GROUP BY age_category, ir.readmission_30d
-- ORDER BY age_category, ir.readmission_30d;

-- Example 5: Distribution of time to readmission
-- SELECT
--     CASE
--         WHEN days_to_next_icu <= 7 THEN '0-7 days'
--         WHEN days_to_next_icu <= 14 THEN '8-14 days'
--         WHEN days_to_next_icu <= 21 THEN '15-21 days'
--         WHEN days_to_next_icu <= 30 THEN '22-30 days'
--         ELSE '>30 days'
--     END AS time_category,
--     COUNT(*) as n_readmissions
-- FROM icu_readmission_30d
-- WHERE readmission_30d = 1
-- GROUP BY time_category
-- ORDER BY time_category;

-- Example 6: Join with admissions for diagnosis/demographic analysis
-- SELECT
--     ir.icustay_id,
--     ir.subject_id,
--     ir.readmission_30d,
--     ir.days_to_next_icu,
--     adm.admission_type,
--     adm.ethnicity,
--     adm.diagnosis
-- FROM icu_readmission_30d ir
-- INNER JOIN admissions adm ON ir.hadm_id = adm.hadm_id
-- WHERE ir.readmission_30d = 1;

-- Example 7: Create combined cohort table with age and readmission
-- CREATE TABLE icu_cohort AS
-- SELECT
--     ia.icustay_id,
--     ia.subject_id,
--     ia.hadm_id,
--     ia.age,
--     ia.gender,
--     ia.icu_los_days,
--     ia.icustay_num,
--     ir.readmission_30d,
--     ir.days_to_next_icu,
--     ir.is_last_icu_stay
-- FROM icu_age ia
-- INNER JOIN icu_readmission_30d ir ON ia.icustay_id = ir.icustay_id
-- WHERE ia.age_raw >= 16;  -- Adults only

-- Example 8: Readmission rate by ICU stay sequence
-- SELECT
--     ia.icustay_num,
--     SUM(ir.readmission_30d) as readmissions,
--     COUNT(*) as total_stays,
--     ROUND(100.0 * SUM(ir.readmission_30d) / COUNT(*), 2) as readmission_rate_pct
-- FROM icu_readmission_30d ir
-- INNER JOIN icu_age ia ON ir.icustay_id = ia.icustay_id
-- WHERE ir.is_last_icu_stay = 0  -- Can't count readmission for last stay
-- GROUP BY ia.icustay_num
-- ORDER BY ia.icustay_num;


-- =====================================================================
-- DIAGNOSTIC QUERIES - Understanding your data
-- =====================================================================

-- Query 1: Overall readmission statistics
-- SELECT
--     COUNT(*) as total_icu_stays,
--     SUM(readmission_30d) as readmissions_30d,
--     ROUND(100.0 * SUM(readmission_30d) / COUNT(*), 2) as readmission_rate_pct,
--     SUM(is_last_icu_stay) as last_icu_stays,
--     COUNT(*) - SUM(is_last_icu_stay) as icu_stays_at_risk
-- FROM icu_readmission_30d;

-- Query 2: Readmission rate excluding last stays (more accurate)
-- SELECT
--     COUNT(*) as icu_stays_at_risk,
--     SUM(readmission_30d) as readmissions_30d,
--     ROUND(100.0 * SUM(readmission_30d) / COUNT(*), 2) as readmission_rate_pct,
--     ROUND(AVG(days_to_next_icu), 2) as mean_days_to_readmit,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_to_next_icu), 2) as median_days_to_readmit
-- FROM icu_readmission_30d
-- WHERE is_last_icu_stay = 0;

-- Query 3: Check for same-hospital transfers vs true readmissions
-- SELECT
--     CASE
--         WHEN next_hadm_id = hadm_id THEN 'Same hospital admission (transfer)'
--         WHEN next_hadm_id IS NOT NULL THEN 'Different hospital admission (readmission)'
--         ELSE 'No next ICU stay'
--     END AS readmission_type,
--     COUNT(*) as n_stays,
--     SUM(readmission_30d) as n_readmissions_30d
-- FROM icu_readmission_30d
-- GROUP BY readmission_type;

-- Query 4: Time to readmission distribution
-- SELECT
--     ROUND(days_to_next_icu, 0) as days,
--     COUNT(*) as n_readmissions
-- FROM icu_readmission_30d
-- WHERE days_to_next_icu <= 30
-- GROUP BY ROUND(days_to_next_icu, 0)
-- ORDER BY days;

-- Query 5: Patients with multiple readmissions
-- SELECT
--     subject_id,
--     COUNT(*) as total_icu_stays,
--     SUM(readmission_30d) as n_readmissions_30d,
--     ROUND(100.0 * SUM(readmission_30d) / COUNT(*), 2) as personal_readmission_rate
-- FROM icu_readmission_30d
-- WHERE is_last_icu_stay = 0
-- GROUP BY subject_id
-- HAVING SUM(readmission_30d) >= 2
-- ORDER BY n_readmissions_30d DESC;
