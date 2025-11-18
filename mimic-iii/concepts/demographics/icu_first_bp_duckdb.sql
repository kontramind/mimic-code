-- =====================================================================
-- Title: First Blood Pressure per ICU Stay (DuckDB)
-- Description: Extracts first systolic and diastolic BP per ICU stay
--              Unit of analysis: ICU stays (icustay_id)
-- MIMIC version: MIMIC-III v1.4
-- Database: DuckDB
-- =====================================================================

-- IMPORTANT NOTES:
-- 1. "First" = earliest measurement after ICU admission (intime)
-- 2. Systolic and diastolic are independent - may be from different times
-- 3. ITEMIDs are configurable - edit the lists below to customize
-- 4. Value range filters: SBP (0-400 mmHg), DBP (0-300 mmHg)
-- 5. Includes ITEMID for invasive vs non-invasive tracking

-- =====================================================================
-- CONFIGURE ITEMIDS HERE
-- =====================================================================
-- SYSTOLIC BP ITEMIDs (edit this list as needed):
-- Invasive (Arterial Line):
--   51      - Arterial BP [Systolic] (CareVue)
--   6701    - Arterial BP #2 [Systolic] (CareVue)
--   220050  - Arterial Blood Pressure systolic (MetaVision)
-- Non-Invasive (Cuff):
--   442     - Manual BP [Systolic] (CareVue)
--   455     - NBP [Systolic] (CareVue)
--   220179  - Non Invasive Blood Pressure systolic (MetaVision)

-- DIASTOLIC BP ITEMIDs (edit this list as needed):
-- Invasive (Arterial Line):
--   8368    - Arterial BP [Diastolic] (CareVue)
--   8555    - Arterial BP #2 [Diastolic] (CareVue)
--   220051  - Arterial Blood Pressure diastolic (MetaVision)
-- Non-Invasive (Cuff):
--   8440    - Manual BP [Diastolic] (CareVue)
--   8441    - NBP [Diastolic] (CareVue)
--   220180  - Non Invasive Blood Pressure diastolic (MetaVision)

-- OUTPUT COLUMNS:
--   icustay_id              : Primary key - unique ICU stay identifier
--   subject_id              : Patient identifier
--   hadm_id                 : Hospital admission identifier
--   icu_intime              : ICU admission timestamp
--   icu_outtime             : ICU discharge timestamp
--
--   SYSTOLIC BP:
--   sysbp_first             : First systolic BP (mmHg)
--   sysbp_first_time        : Timestamp of first SBP measurement
--   sysbp_first_itemid      : ITEMID used (51/6701/220050 = invasive)
--   minutes_to_sysbp        : Minutes from ICU admission to first SBP
--
--   DIASTOLIC BP:
--   diasbp_first            : First diastolic BP (mmHg)
--   diasbp_first_time       : Timestamp of first DBP measurement
--   diasbp_first_itemid     : ITEMID used (8368/8555/220051 = invasive)
--   minutes_to_diasbp       : Minutes from ICU admission to first DBP

-- =====================================================================
-- CREATE MATERIALIZED TABLE
-- =====================================================================

DROP TABLE IF EXISTS icu_first_bp;
CREATE TABLE icu_first_bp AS
WITH bp_measurements AS (
    -- ====================================================================
    -- SYSTOLIC BLOOD PRESSURE (mmHg)
    -- Valid range: 0-400 mmHg
    -- ====================================================================
    SELECT
        ie.icustay_id,
        ie.subject_id,
        ie.hadm_id,
        ie.intime AS icu_intime,
        ie.outtime AS icu_outtime,
        ce.charttime,
        ce.itemid,
        ce.valuenum,
        'sysbp' AS bp_type,
        ROW_NUMBER() OVER (
            PARTITION BY ie.icustay_id
            ORDER BY ce.charttime
        ) AS rn
    FROM icustays ie
    INNER JOIN chartevents ce
        ON ie.icustay_id = ce.icustay_id
    WHERE ce.itemid IN (
            -- ========================================================
            -- EDIT THIS LIST to configure which ITEMIDs to include
            -- ========================================================
            -- Invasive (Arterial Line)
            51,     -- Arterial BP [Systolic] (CareVue)
            6701,   -- Arterial BP #2 [Systolic] (CareVue)
            220050, -- Arterial Blood Pressure systolic (MetaVision)
            -- Non-Invasive (Cuff)
            442,    -- Manual BP [Systolic] (CareVue)
            455,    -- NBP [Systolic] (CareVue)
            220179  -- Non Invasive Blood Pressure systolic (MetaVision)
        )
        AND ce.valuenum IS NOT NULL
        AND (ce.error IS NULL OR ce.error = 0)
        -- Value range filter: 0 < SBP < 400 mmHg
        AND ce.valuenum > 0
        AND ce.valuenum < 400
        -- TIME WINDOW - ROUTINE vital sign pattern
        AND ce.charttime >= ie.intime - INTERVAL '6' HOUR
        AND ce.charttime <= ie.outtime

    UNION ALL

    -- ====================================================================
    -- DIASTOLIC BLOOD PRESSURE (mmHg)
    -- Valid range: 0-300 mmHg
    -- ====================================================================
    SELECT
        ie.icustay_id,
        ie.subject_id,
        ie.hadm_id,
        ie.intime AS icu_intime,
        ie.outtime AS icu_outtime,
        ce.charttime,
        ce.itemid,
        ce.valuenum,
        'diasbp' AS bp_type,
        ROW_NUMBER() OVER (
            PARTITION BY ie.icustay_id
            ORDER BY ce.charttime
        ) AS rn
    FROM icustays ie
    INNER JOIN chartevents ce
        ON ie.icustay_id = ce.icustay_id
    WHERE ce.itemid IN (
            -- ========================================================
            -- EDIT THIS LIST to configure which ITEMIDs to include
            -- ========================================================
            -- Invasive (Arterial Line)
            8368,   -- Arterial BP [Diastolic] (CareVue)
            8555,   -- Arterial BP #2 [Diastolic] (CareVue)
            220051, -- Arterial Blood Pressure diastolic (MetaVision)
            -- Non-Invasive (Cuff)
            8440,   -- Manual BP [Diastolic] (CareVue)
            8441,   -- NBP [Diastolic] (CareVue)
            220180  -- Non Invasive Blood Pressure diastolic (MetaVision)
        )
        AND ce.valuenum IS NOT NULL
        AND (ce.error IS NULL OR ce.error = 0)
        -- Value range filter: 0 < DBP < 300 mmHg
        AND ce.valuenum > 0
        AND ce.valuenum < 300
        -- TIME WINDOW - ROUTINE vital sign pattern
        AND ce.charttime >= ie.intime - INTERVAL '6' HOUR
        AND ce.charttime <= ie.outtime
)
-- ====================================================================
-- PIVOT: Convert rows to columns (one row per ICU stay)
-- ====================================================================
SELECT
    icustay_id,
    subject_id,
    hadm_id,
    icu_intime,
    icu_outtime,

    -- SYSTOLIC BP
    MAX(CASE WHEN bp_type = 'sysbp' AND rn = 1 THEN valuenum END) AS sysbp_first,
    MAX(CASE WHEN bp_type = 'sysbp' AND rn = 1 THEN charttime END) AS sysbp_first_time,
    MAX(CASE WHEN bp_type = 'sysbp' AND rn = 1 THEN itemid END) AS sysbp_first_itemid,
    MAX(CASE WHEN bp_type = 'sysbp' AND rn = 1
        THEN DATE_DIFF('second', icu_intime, charttime) / 60.0 END) AS minutes_to_sysbp,

    -- DIASTOLIC BP
    MAX(CASE WHEN bp_type = 'diasbp' AND rn = 1 THEN valuenum END) AS diasbp_first,
    MAX(CASE WHEN bp_type = 'diasbp' AND rn = 1 THEN charttime END) AS diasbp_first_time,
    MAX(CASE WHEN bp_type = 'diasbp' AND rn = 1 THEN itemid END) AS diasbp_first_itemid,
    MAX(CASE WHEN bp_type = 'diasbp' AND rn = 1
        THEN DATE_DIFF('second', icu_intime, charttime) / 60.0 END) AS minutes_to_diasbp

FROM bp_measurements
WHERE rn = 1
GROUP BY icustay_id, subject_id, hadm_id, icu_intime, icu_outtime
ORDER BY icustay_id;


-- =====================================================================
-- USAGE EXAMPLES
-- =====================================================================

-- Example 1: Simple query - all ICU stays with BP
-- SELECT * FROM icu_first_bp LIMIT 10;

-- Example 2: Check data completeness
-- SELECT
--     COUNT(*) as total_icu_stays,
--     SUM(CASE WHEN sysbp_first IS NOT NULL THEN 1 ELSE 0 END) as has_sbp,
--     SUM(CASE WHEN diasbp_first IS NOT NULL THEN 1 ELSE 0 END) as has_dbp,
--     SUM(CASE WHEN sysbp_first IS NOT NULL AND diasbp_first IS NOT NULL THEN 1 ELSE 0 END) as has_both,
--     ROUND(100.0 * SUM(CASE WHEN sysbp_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_sbp,
--     ROUND(100.0 * SUM(CASE WHEN diasbp_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_dbp
-- FROM icu_first_bp;

-- Example 3: Identify invasive vs non-invasive BP monitoring
-- SELECT
--     icustay_id,
--     sysbp_first,
--     sysbp_first_itemid,
--     CASE
--         WHEN sysbp_first_itemid IN (51, 6701, 220050) THEN 'Invasive (Arterial)'
--         WHEN sysbp_first_itemid IN (442, 455, 220179) THEN 'Non-Invasive (Cuff)'
--         ELSE NULL
--     END AS sbp_measurement_type,
--     diasbp_first,
--     diasbp_first_itemid,
--     CASE
--         WHEN diasbp_first_itemid IN (8368, 8555, 220051) THEN 'Invasive (Arterial)'
--         WHEN diasbp_first_itemid IN (8440, 8441, 220180) THEN 'Non-Invasive (Cuff)'
--         ELSE NULL
--     END AS dbp_measurement_type
-- FROM icu_first_bp
-- WHERE sysbp_first IS NOT NULL OR diasbp_first IS NOT NULL
-- LIMIT 100;

-- Example 4: Calculate pulse pressure (SBP - DBP)
-- SELECT
--     icustay_id,
--     sysbp_first,
--     diasbp_first,
--     sysbp_first - diasbp_first AS pulse_pressure,
--     -- Flag wide pulse pressure (>60 mmHg) or narrow (<25 mmHg)
--     CASE
--         WHEN sysbp_first - diasbp_first > 60 THEN 'Wide (>60)'
--         WHEN sysbp_first - diasbp_first < 25 THEN 'Narrow (<25)'
--         ELSE 'Normal (25-60)'
--     END AS pulse_pressure_category
-- FROM icu_first_bp
-- WHERE sysbp_first IS NOT NULL AND diasbp_first IS NOT NULL;

-- Example 5: Join with icu_age for demographic analysis
-- SELECT
--     ia.icustay_id,
--     ia.age,
--     ia.gender,
--     bp.sysbp_first,
--     bp.diasbp_first,
--     bp.minutes_to_sysbp,
--     bp.minutes_to_diasbp
-- FROM icu_first_bp bp
-- INNER JOIN icu_age ia ON bp.icustay_id = ia.icustay_id
-- WHERE ia.age_raw >= 16
-- LIMIT 100;

-- Example 6: Join with readmission table
-- SELECT
--     bp.icustay_id,
--     bp.sysbp_first,
--     bp.diasbp_first,
--     ir.leads_to_readmission_30d,
--     ir.is_readmission_30d
-- FROM icu_first_bp bp
-- INNER JOIN icu_readmission_30d ir ON bp.icustay_id = ir.icustay_id
-- WHERE bp.sysbp_first IS NOT NULL;

-- Example 7: Create combined cohort with age, readmission, and BP
-- CREATE TABLE icu_cohort AS
-- SELECT
--     ia.icustay_id,
--     ia.subject_id,
--     ia.hadm_id,
--     ia.age,
--     ia.gender,
--     ia.icu_los_days,
--     ir.leads_to_readmission_30d,
--     ir.is_readmission_30d,
--     bp.sysbp_first,
--     bp.diasbp_first,
--     bp.sysbp_first_itemid,
--     bp.diasbp_first_itemid
-- FROM icu_age ia
-- INNER JOIN icu_readmission_30d ir ON ia.icustay_id = ir.icustay_id
-- INNER JOIN icu_first_bp bp ON ia.icustay_id = bp.icustay_id
-- WHERE ia.age_raw >= 16;

-- Example 8: Filter by time to first measurement
-- SELECT *
-- FROM icu_first_bp
-- WHERE minutes_to_sysbp <= 60  -- BP measured within 1 hour of admission
--   AND minutes_to_diasbp <= 60;


-- =====================================================================
-- DIAGNOSTIC QUERIES
-- =====================================================================

-- Query 1: BP measurement availability
-- SELECT
--     COUNT(*) as total_icu_stays,
--     SUM(CASE WHEN sysbp_first IS NOT NULL THEN 1 ELSE 0 END) as n_with_sbp,
--     SUM(CASE WHEN diasbp_first IS NOT NULL THEN 1 ELSE 0 END) as n_with_dbp,
--     SUM(CASE WHEN sysbp_first IS NOT NULL AND diasbp_first IS NOT NULL THEN 1 ELSE 0 END) as n_with_both,
--     ROUND(100.0 * SUM(CASE WHEN sysbp_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_sbp,
--     ROUND(100.0 * SUM(CASE WHEN diasbp_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_dbp,
--     ROUND(AVG(sysbp_first), 1) as mean_sbp,
--     ROUND(AVG(diasbp_first), 1) as mean_dbp,
--     ROUND(AVG(minutes_to_sysbp), 1) as mean_minutes_to_sbp,
--     ROUND(AVG(minutes_to_diasbp), 1) as mean_minutes_to_dbp
-- FROM icu_first_bp;

-- Query 2: Distribution of BP values
-- SELECT
--     ROUND(AVG(sysbp_first), 1) as mean_sbp,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sysbp_first), 1) as median_sbp,
--     ROUND(MIN(sysbp_first), 1) as min_sbp,
--     ROUND(MAX(sysbp_first), 1) as max_sbp,
--     ROUND(AVG(diasbp_first), 1) as mean_dbp,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY diasbp_first), 1) as median_dbp,
--     ROUND(MIN(diasbp_first), 1) as min_dbp,
--     ROUND(MAX(diasbp_first), 1) as max_dbp
-- FROM icu_first_bp;

-- Query 3: Invasive vs non-invasive BP monitoring rates
-- SELECT
--     CASE
--         WHEN sysbp_first_itemid IN (51, 6701, 220050) THEN 'Invasive (Arterial)'
--         WHEN sysbp_first_itemid IN (442, 455, 220179) THEN 'Non-Invasive (Cuff)'
--         ELSE 'No SBP'
--     END AS bp_type,
--     COUNT(*) as n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as pct,
--     ROUND(AVG(sysbp_first), 1) as mean_sbp
-- FROM icu_first_bp
-- GROUP BY bp_type
-- ORDER BY n_stays DESC;

-- Query 4: Check if SBP and DBP are from same time
-- SELECT
--     COUNT(*) as total_with_both,
--     SUM(CASE WHEN sysbp_first_time = diasbp_first_time THEN 1 ELSE 0 END) as same_time,
--     SUM(CASE WHEN ABS(DATE_DIFF('second', sysbp_first_time, diasbp_first_time)) <= 60 THEN 1 ELSE 0 END) as within_1min,
--     SUM(CASE WHEN ABS(DATE_DIFF('second', sysbp_first_time, diasbp_first_time)) <= 300 THEN 1 ELSE 0 END) as within_5min,
--     ROUND(100.0 * SUM(CASE WHEN sysbp_first_time = diasbp_first_time THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_same_time
-- FROM icu_first_bp
-- WHERE sysbp_first IS NOT NULL AND diasbp_first IS NOT NULL;

-- Query 5: ITEMID distribution
-- SELECT
--     sysbp_first_itemid as itemid,
--     'Systolic' as bp_type,
--     COUNT(*) as n_measurements,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY 'Systolic'), 1) as pct
-- FROM icu_first_bp
-- WHERE sysbp_first IS NOT NULL
-- GROUP BY sysbp_first_itemid
-- UNION ALL
-- SELECT
--     diasbp_first_itemid as itemid,
--     'Diastolic' as bp_type,
--     COUNT(*) as n_measurements,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY 'Diastolic'), 1) as pct
-- FROM icu_first_bp
-- WHERE diasbp_first IS NOT NULL
-- GROUP BY diasbp_first_itemid
-- ORDER BY bp_type, n_measurements DESC;
