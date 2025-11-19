-- =====================================================================
-- Title: First Vital Signs per ICU Stay (DuckDB)
-- Description: Extracts first measurement of each vital sign per ICU stay
--              Unit of analysis: ICU stays (icustay_id)
--              Includes: HR, SBP, DBP, MAP, SpO2, RR, Temp, Glucose
-- MIMIC version: MIMIC-III v1.4
-- Database: DuckDB
-- =====================================================================

-- CLINICAL CONTEXT:
-- First vital signs are important for:
-- - Baseline physiological assessment
-- - Severity of illness scores (SOFA, APACHE, etc.)
-- - Early warning scores
-- - ICU admission triage

-- IMPORTANT NOTES:
-- 1. "First" = earliest measurement after ICU admission (intime)
-- 2. Each vital is independent - may be measured at different times
-- 3. Includes both invasive and non-invasive measurements
-- 4. Temperature: Fahrenheit converted to Celsius
-- 5. Value range filters applied for data quality
-- 6. ITEMIDs tracked for invasive vs non-invasive distinction

-- OUTPUT COLUMNS:
--   icustay_id              : Primary key - unique ICU stay identifier
--   subject_id              : Patient identifier
--   hadm_id                 : Hospital admission identifier
--   icu_intime              : ICU admission timestamp
--   icu_outtime             : ICU discharge timestamp
--
--   HEART RATE:
--   heartrate_first         : First heart rate (bpm)
--   heartrate_first_time    : Timestamp of first HR measurement
--   heartrate_first_itemid  : ITEMID used
--   minutes_to_heartrate    : Minutes from ICU admission to first HR
--
--   SYSTOLIC BLOOD PRESSURE:
--   sysbp_first             : First systolic BP (mmHg)
--   sysbp_first_time        : Timestamp
--   sysbp_first_itemid      : ITEMID (51/6701/220050 = invasive)
--   minutes_to_sysbp        : Minutes from ICU admission
--
--   DIASTOLIC BLOOD PRESSURE:
--   diasbp_first            : First diastolic BP (mmHg)
--   diasbp_first_time       : Timestamp
--   diasbp_first_itemid     : ITEMID (8368/8555/220051 = invasive)
--   minutes_to_diasbp       : Minutes from ICU admission
--
--   MEAN ARTERIAL PRESSURE:
--   meanbp_first            : First MAP (mmHg)
--   meanbp_first_time       : Timestamp
--   meanbp_first_itemid     : ITEMID (52/6702/220052/225312 = invasive)
--   minutes_to_meanbp       : Minutes from ICU admission
--
--   SPO2 (Oxygen Saturation):
--   spo2_first              : First SpO2 (%)
--   spo2_first_time         : Timestamp
--   spo2_first_itemid       : ITEMID
--   minutes_to_spo2         : Minutes from ICU admission
--
--   RESPIRATORY RATE:
--   resprate_first          : First respiratory rate (breaths/min)
--   resprate_first_time     : Timestamp
--   resprate_first_itemid   : ITEMID
--   minutes_to_resprate     : Minutes from ICU admission
--
--   TEMPERATURE (always Celsius):
--   tempc_first             : First temperature (°C)
--   tempc_first_time        : Timestamp
--   tempc_first_itemid      : ITEMID (223761/678 were °F, converted)
--   minutes_to_tempc        : Minutes from ICU admission
--
--   GLUCOSE:
--   glucose_first           : First glucose (mg/dL)
--   glucose_first_time      : Timestamp
--   glucose_first_itemid    : ITEMID
--   minutes_to_glucose      : Minutes from ICU admission

-- =====================================================================
-- CREATE MATERIALIZED TABLE (Recommended)
-- =====================================================================

DROP TABLE IF EXISTS icu_first_vitals;
CREATE TABLE icu_first_vitals AS
WITH all_vitals AS (
    -- ====================================================================
    -- HEART RATE (bpm)
    -- Valid range: 0-300 bpm
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
        'heartrate' AS vital_type,
        ROW_NUMBER() OVER (
            PARTITION BY ie.icustay_id
            ORDER BY ce.charttime
        ) AS rn
    FROM icustays ie
    INNER JOIN chartevents ce
        ON ie.icustay_id = ce.icustay_id
    WHERE ce.itemid IN (
            211,    -- Heart Rate (CareVue)
            220045  -- Heart Rate (MetaVision)
        )
        AND ce.valuenum IS NOT NULL
        AND (ce.error IS NULL OR ce.error = 0)
        -- Value range filter: 0 < HR < 300
        AND ce.valuenum > 0
        AND ce.valuenum < 300

    UNION ALL

    -- ====================================================================
    -- SYSTOLIC BLOOD PRESSURE (mmHg)
    -- Valid range: 0-400 mmHg
    -- Includes invasive (arterial) and non-invasive (cuff) measurements
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
        'sysbp' AS vital_type,
        ROW_NUMBER() OVER (
            PARTITION BY ie.icustay_id
            ORDER BY ce.charttime
        ) AS rn
    FROM icustays ie
    INNER JOIN chartevents ce
        ON ie.icustay_id = ce.icustay_id
    WHERE ce.itemid IN (
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
        -- Value range filter: 0 < SBP < 400
        AND ce.valuenum > 0
        AND ce.valuenum < 400

    UNION ALL

    -- ====================================================================
    -- DIASTOLIC BLOOD PRESSURE (mmHg)
    -- Valid range: 0-300 mmHg
    -- Includes invasive (arterial) and non-invasive (cuff) measurements
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
        'diasbp' AS vital_type,
        ROW_NUMBER() OVER (
            PARTITION BY ie.icustay_id
            ORDER BY ce.charttime
        ) AS rn
    FROM icustays ie
    INNER JOIN chartevents ce
        ON ie.icustay_id = ce.icustay_id
    WHERE ce.itemid IN (
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
        -- Value range filter: 0 < DBP < 300
        AND ce.valuenum > 0
        AND ce.valuenum < 300

    UNION ALL

    -- ====================================================================
    -- MEAN ARTERIAL PRESSURE (mmHg)
    -- Valid range: 0-300 mmHg
    -- Includes invasive (arterial) and non-invasive measurements
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
        'meanbp' AS vital_type,
        ROW_NUMBER() OVER (
            PARTITION BY ie.icustay_id
            ORDER BY ce.charttime
        ) AS rn
    FROM icustays ie
    INNER JOIN chartevents ce
        ON ie.icustay_id = ce.icustay_id
    WHERE ce.itemid IN (
            -- Invasive (Arterial Line)
            52,     -- Arterial BP Mean (CareVue)
            6702,   -- Arterial BP Mean #2 (CareVue)
            220052, -- Arterial Blood Pressure mean (MetaVision)
            225312, -- ART BP mean (MetaVision)
            -- Non-Invasive
            456,    -- NBP Mean (CareVue)
            443,    -- Manual BP Mean (calc) (CareVue)
            220181  -- Non Invasive Blood Pressure mean (MetaVision)
        )
        AND ce.valuenum IS NOT NULL
        AND (ce.error IS NULL OR ce.error = 0)
        -- Value range filter: 0 < MAP < 300
        AND ce.valuenum > 0
        AND ce.valuenum < 300

    UNION ALL

    -- ====================================================================
    -- SPO2 - Oxygen Saturation (%)
    -- Valid range: 0-100%
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
        'spo2' AS vital_type,
        ROW_NUMBER() OVER (
            PARTITION BY ie.icustay_id
            ORDER BY ce.charttime
        ) AS rn
    FROM icustays ie
    INNER JOIN chartevents ce
        ON ie.icustay_id = ce.icustay_id
    WHERE ce.itemid IN (
            646,    -- SpO2 (CareVue)
            220277  -- SpO2 (MetaVision)
        )
        AND ce.valuenum IS NOT NULL
        AND (ce.error IS NULL OR ce.error = 0)
        -- Value range filter: 0 < SpO2 <= 100
        AND ce.valuenum > 0
        AND ce.valuenum <= 100

    UNION ALL

    -- ====================================================================
    -- RESPIRATORY RATE (breaths/min)
    -- Valid range: 0-70 breaths/min
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
        'resprate' AS vital_type,
        ROW_NUMBER() OVER (
            PARTITION BY ie.icustay_id
            ORDER BY ce.charttime
        ) AS rn
    FROM icustays ie
    INNER JOIN chartevents ce
        ON ie.icustay_id = ce.icustay_id
    WHERE ce.itemid IN (
            615,    -- Resp Rate (Total) (CareVue)
            618,    -- Respiratory Rate (CareVue)
            220210, -- Respiratory Rate (MetaVision)
            224690  -- Respiratory Rate (Total) (MetaVision)
        )
        AND ce.valuenum IS NOT NULL
        AND (ce.error IS NULL OR ce.error = 0)
        -- Value range filter: 0 < RR < 70
        AND ce.valuenum > 0
        AND ce.valuenum < 70

    UNION ALL

    -- ====================================================================
    -- TEMPERATURE - Always converted to Celsius (°C)
    -- Valid range: 10-50°C (50-122°F)
    -- Fahrenheit values automatically converted using (F-32)/1.8
    -- ====================================================================
    SELECT
        ie.icustay_id,
        ie.subject_id,
        ie.hadm_id,
        ie.intime AS icu_intime,
        ie.outtime AS icu_outtime,
        ce.charttime,
        ce.itemid,
        -- Convert Fahrenheit to Celsius
        CASE
            WHEN ce.itemid IN (223761, 678) THEN (ce.valuenum - 32) / 1.8
            ELSE ce.valuenum
        END AS valuenum,
        'tempc' AS vital_type,
        ROW_NUMBER() OVER (
            PARTITION BY ie.icustay_id
            ORDER BY ce.charttime
        ) AS rn
    FROM icustays ie
    INNER JOIN chartevents ce
        ON ie.icustay_id = ce.icustay_id
    WHERE ce.itemid IN (
            -- Celsius
            223762, -- Temperature Celsius (MetaVision)
            676,    -- Temperature C (CareVue)
            -- Fahrenheit (will be converted)
            223761, -- Temperature Fahrenheit (MetaVision)
            678     -- Temperature F (CareVue)
        )
        AND ce.valuenum IS NOT NULL
        AND (ce.error IS NULL OR ce.error = 0)
        -- Value range filter (after conversion to Celsius): 10 < Temp < 50
        AND (
            (ce.itemid IN (223762, 676) AND ce.valuenum > 10 AND ce.valuenum < 50) OR
            (ce.itemid IN (223761, 678) AND ce.valuenum > 70 AND ce.valuenum < 120)
        )

    UNION ALL

    -- ====================================================================
    -- GLUCOSE (mg/dL)
    -- Valid range: > 0 (no upper limit in existing MIMIC code)
    -- Includes lab values and fingerstick measurements
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
        'glucose' AS vital_type,
        ROW_NUMBER() OVER (
            PARTITION BY ie.icustay_id
            ORDER BY ce.charttime
        ) AS rn
    FROM icustays ie
    INNER JOIN chartevents ce
        ON ie.icustay_id = ce.icustay_id
    WHERE ce.itemid IN (
            807,    -- Fingerstick Glucose (CareVue)
            811,    -- Glucose (70-105) (CareVue)
            1529,   -- Glucose (CareVue)
            3745,   -- BloodGlucose (CareVue)
            3744,   -- Blood Glucose (CareVue)
            225664, -- Glucose finger stick (MetaVision)
            220621, -- Glucose (serum) (MetaVision)
            226537  -- Glucose (whole blood) (MetaVision)
        )
        AND ce.valuenum IS NOT NULL
        AND (ce.error IS NULL OR ce.error = 0)
        -- Value range filter: Glucose > 0
        AND ce.valuenum > 0
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

    -- HEART RATE
    MAX(CASE WHEN vital_type = 'heartrate' AND rn = 1 THEN valuenum END) AS heartrate_first,
    MAX(CASE WHEN vital_type = 'heartrate' AND rn = 1 THEN charttime END) AS heartrate_first_time,
    MAX(CASE WHEN vital_type = 'heartrate' AND rn = 1 THEN itemid END) AS heartrate_first_itemid,
    MAX(CASE WHEN vital_type = 'heartrate' AND rn = 1
        THEN DATE_DIFF('second', icu_intime, charttime) / 60.0 END) AS minutes_to_heartrate,

    -- SYSTOLIC BP
    MAX(CASE WHEN vital_type = 'sysbp' AND rn = 1 THEN valuenum END) AS sysbp_first,
    MAX(CASE WHEN vital_type = 'sysbp' AND rn = 1 THEN charttime END) AS sysbp_first_time,
    MAX(CASE WHEN vital_type = 'sysbp' AND rn = 1 THEN itemid END) AS sysbp_first_itemid,
    MAX(CASE WHEN vital_type = 'sysbp' AND rn = 1
        THEN DATE_DIFF('second', icu_intime, charttime) / 60.0 END) AS minutes_to_sysbp,

    -- DIASTOLIC BP
    MAX(CASE WHEN vital_type = 'diasbp' AND rn = 1 THEN valuenum END) AS diasbp_first,
    MAX(CASE WHEN vital_type = 'diasbp' AND rn = 1 THEN charttime END) AS diasbp_first_time,
    MAX(CASE WHEN vital_type = 'diasbp' AND rn = 1 THEN itemid END) AS diasbp_first_itemid,
    MAX(CASE WHEN vital_type = 'diasbp' AND rn = 1
        THEN DATE_DIFF('second', icu_intime, charttime) / 60.0 END) AS minutes_to_diasbp,

    -- MEAN ARTERIAL PRESSURE
    MAX(CASE WHEN vital_type = 'meanbp' AND rn = 1 THEN valuenum END) AS meanbp_first,
    MAX(CASE WHEN vital_type = 'meanbp' AND rn = 1 THEN charttime END) AS meanbp_first_time,
    MAX(CASE WHEN vital_type = 'meanbp' AND rn = 1 THEN itemid END) AS meanbp_first_itemid,
    MAX(CASE WHEN vital_type = 'meanbp' AND rn = 1
        THEN DATE_DIFF('second', icu_intime, charttime) / 60.0 END) AS minutes_to_meanbp,

    -- SPO2
    MAX(CASE WHEN vital_type = 'spo2' AND rn = 1 THEN valuenum END) AS spo2_first,
    MAX(CASE WHEN vital_type = 'spo2' AND rn = 1 THEN charttime END) AS spo2_first_time,
    MAX(CASE WHEN vital_type = 'spo2' AND rn = 1 THEN itemid END) AS spo2_first_itemid,
    MAX(CASE WHEN vital_type = 'spo2' AND rn = 1
        THEN DATE_DIFF('second', icu_intime, charttime) / 60.0 END) AS minutes_to_spo2,

    -- RESPIRATORY RATE
    MAX(CASE WHEN vital_type = 'resprate' AND rn = 1 THEN valuenum END) AS resprate_first,
    MAX(CASE WHEN vital_type = 'resprate' AND rn = 1 THEN charttime END) AS resprate_first_time,
    MAX(CASE WHEN vital_type = 'resprate' AND rn = 1 THEN itemid END) AS resprate_first_itemid,
    MAX(CASE WHEN vital_type = 'resprate' AND rn = 1
        THEN DATE_DIFF('second', icu_intime, charttime) / 60.0 END) AS minutes_to_resprate,

    -- TEMPERATURE (Celsius)
    MAX(CASE WHEN vital_type = 'tempc' AND rn = 1 THEN valuenum END) AS tempc_first,
    MAX(CASE WHEN vital_type = 'tempc' AND rn = 1 THEN charttime END) AS tempc_first_time,
    MAX(CASE WHEN vital_type = 'tempc' AND rn = 1 THEN itemid END) AS tempc_first_itemid,
    MAX(CASE WHEN vital_type = 'tempc' AND rn = 1
        THEN DATE_DIFF('second', icu_intime, charttime) / 60.0 END) AS minutes_to_tempc,

    -- GLUCOSE
    MAX(CASE WHEN vital_type = 'glucose' AND rn = 1 THEN valuenum END) AS glucose_first,
    MAX(CASE WHEN vital_type = 'glucose' AND rn = 1 THEN charttime END) AS glucose_first_time,
    MAX(CASE WHEN vital_type = 'glucose' AND rn = 1 THEN itemid END) AS glucose_first_itemid,
    MAX(CASE WHEN vital_type = 'glucose' AND rn = 1
        THEN DATE_DIFF('second', icu_intime, charttime) / 60.0 END) AS minutes_to_glucose

FROM all_vitals
WHERE rn = 1
GROUP BY icustay_id, subject_id, hadm_id, icu_intime, icu_outtime
ORDER BY icustay_id;


-- =====================================================================
-- USAGE EXAMPLES - Using the icu_first_vitals table
-- =====================================================================

-- Example 1: Simple query - all ICU stays with vitals
-- SELECT * FROM icu_first_vitals LIMIT 10;

-- Example 2: Check data completeness
-- SELECT
--     COUNT(*) as total_icu_stays,
--     SUM(CASE WHEN heartrate_first IS NOT NULL THEN 1 ELSE 0 END) as has_hr,
--     SUM(CASE WHEN sysbp_first IS NOT NULL THEN 1 ELSE 0 END) as has_sbp,
--     SUM(CASE WHEN diasbp_first IS NOT NULL THEN 1 ELSE 0 END) as has_dbp,
--     SUM(CASE WHEN meanbp_first IS NOT NULL THEN 1 ELSE 0 END) as has_map,
--     SUM(CASE WHEN spo2_first IS NOT NULL THEN 1 ELSE 0 END) as has_spo2,
--     SUM(CASE WHEN resprate_first IS NOT NULL THEN 1 ELSE 0 END) as has_rr,
--     SUM(CASE WHEN tempc_first IS NOT NULL THEN 1 ELSE 0 END) as has_temp,
--     SUM(CASE WHEN glucose_first IS NOT NULL THEN 1 ELSE 0 END) as has_glucose
-- FROM icu_first_vitals;

-- Example 3: Join with icu_age for demographic analysis
-- SELECT
--     ia.icustay_id,
--     ia.age,
--     ia.gender,
--     vit.heartrate_first,
--     vit.sysbp_first,
--     vit.diasbp_first,
--     vit.meanbp_first,
--     vit.spo2_first,
--     vit.resprate_first,
--     vit.tempc_first
-- FROM icu_first_vitals vit
-- INNER JOIN icu_age ia ON vit.icustay_id = ia.icustay_id
-- WHERE ia.age_raw >= 16
-- LIMIT 100;

-- Example 4: Identify invasive vs non-invasive BP monitoring
-- SELECT
--     icustay_id,
--     sysbp_first,
--     sysbp_first_itemid,
--     CASE
--         WHEN sysbp_first_itemid IN (51, 6701, 220050) THEN 'Invasive (Arterial)'
--         WHEN sysbp_first_itemid IN (442, 455, 220179) THEN 'Non-Invasive (Cuff)'
--         ELSE 'Unknown'
--     END AS sbp_measurement_type
-- FROM icu_first_vitals
-- WHERE sysbp_first IS NOT NULL
-- LIMIT 100;

-- Example 5: Calculate pulse pressure (SBP - DBP)
-- SELECT
--     icustay_id,
--     sysbp_first,
--     diasbp_first,
--     sysbp_first - diasbp_first AS pulse_pressure,
--     -- Flag wide pulse pressure (>60 mmHg)
--     CASE WHEN sysbp_first - diasbp_first > 60 THEN 1 ELSE 0 END AS wide_pulse_pressure
-- FROM icu_first_vitals
-- WHERE sysbp_first IS NOT NULL AND diasbp_first IS NOT NULL;

-- Example 6: Filter by time to first measurement
-- SELECT *
-- FROM icu_first_vitals
-- WHERE minutes_to_sysbp <= 60  -- BP measured within 1 hour of admission
--   AND minutes_to_heartrate <= 60;

-- Example 7: Create combined cohort with age, readmission, and vitals
-- CREATE TABLE icu_cohort_complete AS
-- SELECT
--     ia.icustay_id,
--     ia.subject_id,
--     ia.hadm_id,
--     ia.age,
--     ia.gender,
--     ia.icu_los_days,
--     ir.leads_to_readmission_30d,
--     ir.is_readmission_30d,
--     vit.heartrate_first,
--     vit.sysbp_first,
--     vit.diasbp_first,
--     vit.meanbp_first,
--     vit.spo2_first,
--     vit.resprate_first,
--     vit.tempc_first,
--     vit.glucose_first
-- FROM icu_age ia
-- INNER JOIN icu_readmission_30d ir ON ia.icustay_id = ir.icustay_id
-- INNER JOIN icu_first_vitals vit ON ia.icustay_id = vit.icustay_id
-- WHERE ia.age_raw >= 16;


-- =====================================================================
-- DIAGNOSTIC QUERIES - Understanding your data
-- =====================================================================

-- Query 1: Overall vital signs availability
-- SELECT
--     'Heart Rate' as vital,
--     COUNT(*) as n_icu_stays,
--     SUM(CASE WHEN heartrate_first IS NOT NULL THEN 1 ELSE 0 END) as n_measured,
--     ROUND(100.0 * SUM(CASE WHEN heartrate_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_measured,
--     ROUND(AVG(heartrate_first), 1) as mean_value,
--     ROUND(AVG(minutes_to_heartrate), 1) as mean_minutes_to_measure
-- FROM icu_first_vitals
-- UNION ALL
-- SELECT 'Systolic BP', COUNT(*), SUM(CASE WHEN sysbp_first IS NOT NULL THEN 1 ELSE 0 END),
--        ROUND(100.0 * SUM(CASE WHEN sysbp_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1),
--        ROUND(AVG(sysbp_first), 1), ROUND(AVG(minutes_to_sysbp), 1)
-- FROM icu_first_vitals
-- UNION ALL
-- SELECT 'Diastolic BP', COUNT(*), SUM(CASE WHEN diasbp_first IS NOT NULL THEN 1 ELSE 0 END),
--        ROUND(100.0 * SUM(CASE WHEN diasbp_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1),
--        ROUND(AVG(diasbp_first), 1), ROUND(AVG(minutes_to_diasbp), 1)
-- FROM icu_first_vitals
-- UNION ALL
-- SELECT 'Mean BP', COUNT(*), SUM(CASE WHEN meanbp_first IS NOT NULL THEN 1 ELSE 0 END),
--        ROUND(100.0 * SUM(CASE WHEN meanbp_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1),
--        ROUND(AVG(meanbp_first), 1), ROUND(AVG(minutes_to_meanbp), 1)
-- FROM icu_first_vitals;

-- Query 2: Distribution of first vital signs
-- SELECT
--     ROUND(AVG(heartrate_first), 1) as mean_hr,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY heartrate_first), 1) as median_hr,
--     ROUND(AVG(sysbp_first), 1) as mean_sbp,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sysbp_first), 1) as median_sbp,
--     ROUND(AVG(diasbp_first), 1) as mean_dbp,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY diasbp_first), 1) as median_dbp,
--     ROUND(AVG(meanbp_first), 1) as mean_map,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY meanbp_first), 1) as median_map,
--     ROUND(AVG(spo2_first), 1) as mean_spo2,
--     ROUND(AVG(tempc_first), 1) as mean_temp
-- FROM icu_first_vitals;

-- Query 3: Check invasive vs non-invasive BP monitoring rates
-- SELECT
--     CASE
--         WHEN sysbp_first_itemid IN (51, 6701, 220050) THEN 'Invasive (Arterial)'
--         WHEN sysbp_first_itemid IN (442, 455, 220179) THEN 'Non-Invasive (Cuff)'
--         ELSE 'No SBP'
--     END AS bp_type,
--     COUNT(*) as n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as pct
-- FROM icu_first_vitals
-- GROUP BY bp_type
-- ORDER BY n_stays DESC;

-- Query 4: Temperature unit distribution (check Fahrenheit conversion)
-- SELECT
--     tempc_first_itemid,
--     CASE
--         WHEN tempc_first_itemid IN (223761, 678) THEN 'Fahrenheit (converted)'
--         WHEN tempc_first_itemid IN (223762, 676) THEN 'Celsius (original)'
--         ELSE 'Unknown'
--     END AS temp_unit,
--     COUNT(*) as n_measurements,
--     ROUND(AVG(tempc_first), 2) as mean_temp_celsius,
--     ROUND(MIN(tempc_first), 2) as min_temp,
--     ROUND(MAX(tempc_first), 2) as max_temp
-- FROM icu_first_vitals
-- WHERE tempc_first IS NOT NULL
-- GROUP BY tempc_first_itemid
-- ORDER BY n_measurements DESC;
