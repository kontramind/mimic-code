-- =====================================================================
-- Title: Complete ICU Stay Data INCLUDING Orphan Stays (DuckDB)
-- Description: Comprehensive dataset starting from icustays base table,
--              including ICU stays without matching admission records
-- MIMIC version: MIMIC-III v1.4
-- Database: DuckDB
-- =====================================================================

-- DIFFERENCE FROM icu_stay_complete:
-- This version uses icustays as the base table with LEFT JOINs to include
-- ALL 61,532 ICU stays, including ~17K "orphan" stays that have no matching
-- record in the admissions table. Use this for complete coverage analysis.
--
-- icu_stay_complete uses icustay_detail (which filters to has_chartevents_data=1
-- and requires admissions match), resulting in ~44K stays.

-- PREREQUISITES:
-- Same as icu_stay_complete, plus:
--   - icustays (base MIMIC-III table)
--   - patients (base MIMIC-III table)

-- =====================================================================
-- CREATE COMPREHENSIVE ICU STAY TABLE (ALL STAYS)
-- =====================================================================

DROP TABLE IF EXISTS icu_stay_complete_with_orphans;

CREATE TABLE icu_stay_complete_with_orphans AS
SELECT
    -- ================================================================
    -- IDENTIFIERS
    -- ================================================================
    ie.icustay_id,
    ie.subject_id,
    ie.hadm_id,

    -- ================================================================
    -- DEMOGRAPHICS
    -- ================================================================
    ia.age,
    COALESCE(id.gender, p.gender) AS gender,
    id.ethnicity_grouped,           -- NULL for orphans
    adm.admission_type,             -- NULL for orphans

    -- ================================================================
    -- READMISSION FLAGS
    -- ================================================================
    ir.leads_to_readmission_30d,
    ir.is_readmission_30d,

    -- ================================================================
    -- MORTALITY INDICATORS
    -- ================================================================
    COALESCE(id.dod, p.dod) AS dod,
    COALESCE(id.hospital_expire_flag, adm.hospital_expire_flag) AS hospital_expire_flag,

    -- Computed: died during ICU stay (intime <= dod <= outtime)
    CASE
        WHEN COALESCE(id.dod, p.dod) IS NOT NULL
        AND COALESCE(id.dod, p.dod) >= ie.intime
        AND COALESCE(id.dod, p.dod) <= ie.outtime
        THEN 1
        ELSE 0
    END AS icu_mortality_flag,

    -- ================================================================
    -- VITAL SIGNS (First measurement)
    -- ================================================================
    hr.hr_first,
    bp.sysbp_first,
    bp.diasbp_first,
    rr.resprate_first,

    -- ================================================================
    -- ANTHROPOMETRICS
    -- ================================================================
    ht.height_first,
    wt.weight_first,

    -- Computed BMI: weight (kg) / height (m)^2
    CASE
        WHEN ht.height_first IS NOT NULL
        AND wt.weight_first IS NOT NULL
        AND ht.height_first > 0
        THEN ROUND(wt.weight_first / ((ht.height_first / 100.0) * (ht.height_first / 100.0)), 1)
        ELSE NULL
    END AS bmi,

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
    ie.intime AS icu_intime,
    ie.outtime AS icu_outtime,
    COALESCE(id.los_icu,
        ROUND(EXTRACT(EPOCH FROM (ie.outtime - ie.intime)) / 86400.0, 2)
    ) AS los_icu,

    -- ================================================================
    -- DATA QUALITY FLAGS
    -- ================================================================
    CASE WHEN adm.hadm_id IS NOT NULL THEN 1 ELSE 0 END AS has_admission_record,
    CASE WHEN id.icustay_id IS NOT NULL THEN 1 ELSE 0 END AS has_icustay_detail

FROM icustays ie

-- Join patients table for basic demographics (always available)
LEFT JOIN patients p
    ON ie.subject_id = p.subject_id

-- Join icustay_detail (may be NULL for orphans or no-chartevents stays)
LEFT JOIN icustay_detail id
    ON ie.icustay_id = id.icustay_id

-- Join admissions (NULL for orphan stays)
LEFT JOIN admissions adm
    ON ie.hadm_id = adm.hadm_id

-- Join age (corrected for >89 years)
LEFT JOIN icu_age ia
    ON ie.icustay_id = ia.icustay_id

-- Join readmission indicators
LEFT JOIN icu_readmission_30d ir
    ON ie.icustay_id = ir.icustay_id

-- Join vital signs
LEFT JOIN icu_first_hr hr
    ON ie.icustay_id = hr.icustay_id

LEFT JOIN icu_first_bp bp
    ON ie.icustay_id = bp.icustay_id

LEFT JOIN icu_first_resprate rr
    ON ie.icustay_id = rr.icustay_id

-- Join anthropometrics
LEFT JOIN icu_first_height ht
    ON ie.icustay_id = ht.icustay_id

LEFT JOIN icu_first_weight wt
    ON ie.icustay_id = wt.icustay_id

-- Join laboratory values
LEFT JOIN icu_first_ntprobnp nt
    ON ie.icustay_id = nt.icustay_id

LEFT JOIN icu_first_creatinine cr
    ON ie.icustay_id = cr.icustay_id

LEFT JOIN icu_first_bun bun
    ON ie.icustay_id = bun.icustay_id

LEFT JOIN icu_first_potassium k
    ON ie.icustay_id = k.icustay_id

LEFT JOIN icu_first_total_cholesterol chol
    ON ie.icustay_id = chol.icustay_id

ORDER BY ie.icustay_id;


-- =====================================================================
-- DIAGNOSTIC QUERIES FOR ORPHAN ANALYSIS
-- =====================================================================

-- Query 1: Compare row counts
-- SELECT
--     'icustays' AS source, COUNT(*) AS n FROM icustays
-- UNION ALL
-- SELECT
--     'icu_stay_complete_with_orphans', COUNT(*) FROM icu_stay_complete_with_orphans
-- UNION ALL
-- SELECT
--     'icu_stay_complete', COUNT(*) FROM icu_stay_complete;

-- Query 2: Orphan vs non-orphan breakdown
-- SELECT
--     has_admission_record,
--     has_icustay_detail,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_stay_complete_with_orphans
-- GROUP BY has_admission_record, has_icustay_detail
-- ORDER BY n_stays DESC;

-- Query 3: Data completeness comparison
-- SELECT
--     has_admission_record,
--     COUNT(*) AS n,
--     ROUND(100.0 * COUNT(age) / COUNT(*), 1) AS pct_age,
--     ROUND(100.0 * COUNT(admission_type) / COUNT(*), 1) AS pct_admission_type,
--     ROUND(100.0 * COUNT(hr_first) / COUNT(*), 1) AS pct_hr,
--     ROUND(100.0 * COUNT(creatinine_first) / COUNT(*), 1) AS pct_creatinine
-- FROM icu_stay_complete_with_orphans
-- GROUP BY has_admission_record;
