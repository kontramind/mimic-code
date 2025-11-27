-- =====================================================================
-- Count Orphaned ICU Stays (without HADMIDs)
-- =====================================================================
-- Description: This query counts ICU stays that do not have matching
--              records in the admissions table (orphan stays)
-- MIMIC version: MIMIC-III v1.4
-- Database: DuckDB
-- =====================================================================

-- Query 1: Count orphan ICU stays (no matching admission record)
SELECT
    COUNT(*) as orphan_icu_stays
FROM icustays ie
LEFT JOIN admissions adm
    ON ie.hadm_id = adm.hadm_id
WHERE adm.hadm_id IS NULL;

-- Query 2: Detailed breakdown of ICU stays by orphan status
SELECT
    CASE
        WHEN adm.hadm_id IS NULL THEN 'Orphan (No HADM_ID match)'
        ELSE 'Has admission record'
    END AS status,
    COUNT(*) as n_stays,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM icustays ie
LEFT JOIN admissions adm
    ON ie.hadm_id = adm.hadm_id
GROUP BY
    CASE
        WHEN adm.hadm_id IS NULL THEN 'Orphan (No HADM_ID match)'
        ELSE 'Has admission record'
    END
ORDER BY n_stays DESC;

-- Query 3: Check for NULL hadm_id in icustays table
SELECT
    COUNT(*) as total_icu_stays,
    COUNT(hadm_id) as icu_stays_with_hadm_id,
    COUNT(*) - COUNT(hadm_id) as icu_stays_with_null_hadm_id,
    ROUND(100.0 * (COUNT(*) - COUNT(hadm_id)) / COUNT(*), 2) as pct_null_hadm_id
FROM icustays;

-- Query 4: Combined analysis - NULL vs not matching
SELECT
    CASE
        WHEN ie.hadm_id IS NULL THEN 'NULL hadm_id'
        WHEN adm.hadm_id IS NULL THEN 'hadm_id present but no match in admissions'
        ELSE 'Has admission record'
    END AS status,
    COUNT(*) as n_stays,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM icustays ie
LEFT JOIN admissions adm
    ON ie.hadm_id = adm.hadm_id
GROUP BY
    CASE
        WHEN ie.hadm_id IS NULL THEN 'NULL hadm_id'
        WHEN adm.hadm_id IS NULL THEN 'hadm_id present but no match in admissions'
        ELSE 'Has admission record'
    END
ORDER BY n_stays DESC;

-- Query 5: Sample orphan ICU stays for inspection
SELECT
    ie.icustay_id,
    ie.subject_id,
    ie.hadm_id,
    ie.intime,
    ie.outtime,
    ie.los as length_of_stay_days,
    p.gender,
    p.dob,
    p.dod
FROM icustays ie
LEFT JOIN admissions adm
    ON ie.hadm_id = adm.hadm_id
LEFT JOIN patients p
    ON ie.subject_id = p.subject_id
WHERE adm.hadm_id IS NULL
LIMIT 20;
