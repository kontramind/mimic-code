-- =====================================================================
-- Title: Clinical ICU Time Boundaries Based on Heart Rate Monitoring (DuckDB)
-- Description: Provides clinically accurate ICU stay times based on actual
--              chart events (heart rate measurements) rather than administrative times
-- MIMIC version: MIMIC-III v1.4
-- Database: DuckDB
-- =====================================================================

-- CLINICAL PURPOSE:
-- Administrative ICU times (intime/outtime in icustays table) represent when
-- patients were officially admitted/discharged, but actual clinical monitoring
-- may start earlier or end later. This table provides:
-- - intime_hr: First heart rate measurement (monitoring started)
-- - outtime_hr: Last heart rate measurement (monitoring ended)
--
-- These clinically accurate times are essential for:
-- - Time-series analysis (hourly/daily aggregations)
-- - "First measurement" extraction (baseline vital signs, labs)
-- - Length of stay calculations based on actual care delivery
-- - Foundation for icustay_hours table (hourly time series)

-- DATA QUALITY METRICS (from MIMIC-III analysis):
-- Coverage:     96.43% of ICU stays have HR measurements
-- NULL rate:    3.57% (2,197 out of 61,532 stays)
-- Density:      Median 48 measurements per stay, Average 114
-- First HR:     Median 0.67 hours (40 min) after ICU admission
-- Last HR:      Median 1.88 hours before ICU discharge
--
-- Clinical vs Administrative Time Difference:
-- - intime_hr typically ~40 minutes AFTER intime (monitoring setup delay)
-- - outtime_hr typically ~2 hours BEFORE outtime (transfer/downgrade)
-- - Effective monitoring window ~2.5 hours shorter than administrative

-- IMPORTANT NOTES:
-- 1. Uses heart rate (HR) as proxy for monitoring activity (most common vital)
-- 2. NULL values (3.57%) represent ICU stays without HR measurements
--    - Likely very short stays, immediate deaths/transfers, or data quality issues
-- 3. Fuzzy boundaries (±12 hours) capture pre-ICU and post-ICU measurements
-- 4. Handles overlapping admissions (sets boundary at midpoint)
-- 5. Required dependency for icustay_hours_duckdb.sql

-- OUTPUT COLUMNS:
--   icustay_id    : Primary key - unique ICU stay identifier
--   subject_id    : Patient identifier
--   hadm_id       : Hospital admission identifier
--   intime_hr     : First heart rate measurement timestamp (clinical monitoring start)
--                   NULL if no HR measurements exist for this ICU stay (3.57%)
--   outtime_hr    : Last heart rate measurement timestamp (clinical monitoring end)
--                   NULL if no HR measurements exist for this ICU stay (3.57%)

-- =====================================================================
-- CREATE MATERIALIZED TABLE (Recommended)
-- =====================================================================
-- This creates a permanent table used as foundation for time-series analyses

DROP TABLE IF EXISTS icustay_times;
CREATE TABLE icustay_times AS
WITH
-- ====================================================================
-- Step 1: Create fuzzy hospital admission boundaries
-- ====================================================================
-- Purpose: Chart events may be recorded slightly before/after admission
-- Solution: Create ±12 hour buffer windows around each admission
-- Special case: If two admissions within 24 hours, set boundary at midpoint
h AS (
    SELECT
        subject_id,
        hadm_id,
        admittime,
        dischtime,
        -- Get previous discharge time for this patient
        LAG(dischtime) OVER (PARTITION BY subject_id ORDER BY admittime) AS dischtime_lag,
        -- Get next admission time for this patient
        LEAD(admittime) OVER (PARTITION BY subject_id ORDER BY admittime) AS admittime_lead
    FROM admissions
),

-- ====================================================================
-- Step 2: Calculate fuzzy start/end boundaries for data extraction
-- ====================================================================
adm AS (
    SELECT
        h.subject_id,
        h.hadm_id,

        -- Data extraction START boundary:
        -- Default: 12 hours before admission
        -- Exception: If previous discharge < 24 hours ago, use midpoint
        CASE
            WHEN h.dischtime_lag IS NOT NULL
            AND h.dischtime_lag > (h.admittime - INTERVAL '24' HOUR)
                -- Previous admission within 24 hours: set boundary at midpoint
                THEN h.admittime - INTERVAL '1' SECOND *
                     CAST(DATE_DIFF('second', h.dischtime_lag, h.admittime) / 2 AS BIGINT)
            ELSE
                -- Standard case: 12 hours before admission
                h.admittime - INTERVAL '12' HOUR
        END AS data_start,

        -- Data extraction END boundary:
        -- Default: 12 hours after discharge
        -- Exception: If next admission < 24 hours away, use midpoint
        CASE
            WHEN h.admittime_lead IS NOT NULL
            AND h.admittime_lead < (h.dischtime + INTERVAL '24' HOUR)
                -- Next admission within 24 hours: set boundary at midpoint
                THEN h.dischtime + INTERVAL '1' SECOND *
                     CAST(DATE_DIFF('second', h.dischtime, h.admittime_lead) / 2 AS BIGINT)
            ELSE
                -- Standard case: 12 hours after discharge
                h.dischtime + INTERVAL '12' HOUR
        END AS data_end
    FROM h
),

-- ====================================================================
-- Step 3: Extract first and last HR measurements per ICU stay
-- ====================================================================
-- Uses fuzzy boundaries to capture HR near ICU admission/discharge
-- Item IDs: 211 (CareVue), 220045 (MetaVision)
hr_measurements AS (
    SELECT
        ce.icustay_id,
        MIN(ce.charttime) AS intime_hr,
        MAX(ce.charttime) AS outtime_hr
    FROM chartevents ce
    -- Join to fuzzy admission boundaries
    INNER JOIN adm
        ON ce.hadm_id = adm.hadm_id
        AND ce.charttime >= adm.data_start
        AND ce.charttime < adm.data_end
    -- Filter for heart rate measurements only
    WHERE ce.itemid IN (
        211,        -- Heart Rate (CareVue)
        220045      -- Heart Rate (MetaVision)
    )
    GROUP BY ce.icustay_id
)

-- ====================================================================
-- Step 4: Join back to all ICU stays (LEFT JOIN to preserve NULLs)
-- ====================================================================
SELECT
    ie.subject_id,
    ie.hadm_id,
    ie.icustay_id,
    hr.intime_hr,
    hr.outtime_hr
FROM icustays ie
LEFT JOIN hr_measurements hr
    ON ie.icustay_id = hr.icustay_id
ORDER BY ie.subject_id, ie.hadm_id, ie.icustay_id;


-- =====================================================================
-- ALTERNATIVE: CREATE VIEW (Lightweight, always up-to-date)
-- =====================================================================
-- Uncomment below if you prefer a VIEW instead of a materialized TABLE
-- VIEWs are lighter but recalculate on every query (slower for chartevents)
-- TABLEs are much faster for repeated queries

-- DROP VIEW IF EXISTS icustay_times;
-- CREATE VIEW icustay_times AS
-- WITH h AS (...) -- same query as above


-- =====================================================================
-- USAGE EXAMPLES - Using the icustay_times table
-- =====================================================================

-- Example 1: Compare administrative vs clinical times
-- Shows how much clinical monitoring differs from administrative records
-- SELECT
--     ie.icustay_id,
--     ie.intime AS admin_intime,
--     it.intime_hr AS clinical_intime,
--     ie.outtime AS admin_outtime,
--     it.outtime_hr AS clinical_outtime,
--     -- Time differences
--     ROUND(DATE_DIFF('minute', ie.intime, it.intime_hr) / 60.0, 2) AS hours_delay_to_first_hr,
--     ROUND(DATE_DIFF('minute', it.outtime_hr, ie.outtime) / 60.0, 2) AS hours_before_discharge_last_hr,
--     -- Length of stay comparison
--     ROUND(DATE_DIFF('hour', ie.intime, ie.outtime) / 24.0, 2) AS los_admin_days,
--     ROUND(DATE_DIFF('hour', it.intime_hr, it.outtime_hr) / 24.0, 2) AS los_clinical_days
-- FROM icustays ie
-- INNER JOIN icustay_times it ON ie.icustay_id = it.icustay_id
-- WHERE it.intime_hr IS NOT NULL  -- Exclude NULLs
-- LIMIT 100;

-- Example 2: Identify ICU stays without HR measurements (NULLs)
-- Useful for understanding data quality issues
-- SELECT
--     ie.icustay_id,
--     ie.subject_id,
--     ie.hadm_id,
--     ie.intime,
--     ie.outtime,
--     DATE_DIFF('hour', ie.intime, ie.outtime) AS los_hours,
--     it.intime_hr,
--     it.outtime_hr,
--     CASE WHEN it.intime_hr IS NULL THEN 'No HR data' ELSE 'Has HR data' END AS hr_status
-- FROM icustays ie
-- LEFT JOIN icustay_times it ON ie.icustay_id = it.icustay_id
-- WHERE it.intime_hr IS NULL
-- ORDER BY los_hours;

-- Example 3: Get vital signs during ACTUAL monitoring period (clinical)
-- More accurate than using administrative times
-- SELECT
--     it.icustay_id,
--     ce.charttime,
--     ce.itemid,
--     ce.valuenum
-- FROM icustay_times it
-- INNER JOIN chartevents ce
--     ON ce.icustay_id = it.icustay_id
-- WHERE it.intime_hr IS NOT NULL
--     AND ce.charttime >= it.intime_hr
--     AND ce.charttime <= it.outtime_hr
--     AND ce.itemid IN (211, 220045)  -- Heart rate
-- LIMIT 100;

-- Example 4: Join with icu_age for comprehensive demographics
-- SELECT
--     ia.icustay_id,
--     ia.subject_id,
--     ia.age,
--     ia.gender,
--     ia.icu_los_days AS los_admin,
--     it.intime_hr,
--     it.outtime_hr,
--     ROUND(DATE_DIFF('hour', it.intime_hr, it.outtime_hr) / 24.0, 2) AS los_clinical_days
-- FROM icu_age ia
-- INNER JOIN icustay_times it ON ia.icustay_id = it.icustay_id
-- WHERE it.intime_hr IS NOT NULL
--     AND ia.age >= 16;

-- Example 5: Extract first lab result after monitoring started
-- SELECT
--     it.icustay_id,
--     it.intime_hr,
--     le.charttime AS first_lab_time,
--     le.itemid,
--     le.valuenum,
--     ROUND(DATE_DIFF('minute', it.intime_hr, le.charttime) / 60.0, 2) AS hours_after_monitoring_start
-- FROM icustay_times it
-- INNER JOIN labevents le
--     ON le.hadm_id = it.hadm_id
-- WHERE it.intime_hr IS NOT NULL
--     AND le.charttime >= it.intime_hr
--     AND le.charttime <= it.intime_hr + INTERVAL '24' HOUR
-- ORDER BY it.icustay_id, le.charttime
-- LIMIT 100;

-- Example 6: Use as foundation for hourly time series
-- This is how icustay_hours_duckdb.sql will use this table
-- SELECT
--     it.icustay_id,
--     it.intime_hr,
--     it.outtime_hr,
--     -- Calculate number of hours from first to last HR
--     CEIL(DATE_DIFF('hour', it.intime_hr, it.outtime_hr)) AS total_monitoring_hours
-- FROM icustay_times it
-- WHERE it.intime_hr IS NOT NULL
-- LIMIT 10;


-- =====================================================================
-- DIAGNOSTIC QUERIES - Data Quality Checks
-- =====================================================================

-- Query 1: Overall coverage statistics
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     COUNT(intime_hr) AS stays_with_hr,
--     COUNT(*) - COUNT(intime_hr) AS stays_without_hr,
--     ROUND(100.0 * COUNT(intime_hr) / COUNT(*), 2) AS pct_coverage
-- FROM icustay_times;

-- Query 2: Time difference distributions (clinical vs administrative)
-- SELECT
--     ROUND(AVG(DATE_DIFF('minute', ie.intime, it.intime_hr) / 60.0), 2) AS avg_hours_delay_first_hr,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATE_DIFF('minute', ie.intime, it.intime_hr) / 60.0), 2) AS median_hours_delay_first_hr,
--     ROUND(AVG(DATE_DIFF('minute', it.outtime_hr, ie.outtime) / 60.0), 2) AS avg_hours_before_discharge_last_hr,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATE_DIFF('minute', it.outtime_hr, ie.outtime) / 60.0), 2) AS median_hours_before_discharge_last_hr
-- FROM icustays ie
-- INNER JOIN icustay_times it ON ie.icustay_id = it.icustay_id
-- WHERE it.intime_hr IS NOT NULL;

-- Query 3: Characteristics of ICU stays without HR data
-- SELECT
--     CASE
--         WHEN DATE_DIFF('hour', ie.intime, ie.outtime) < 1 THEN '<1 hour'
--         WHEN DATE_DIFF('hour', ie.intime, ie.outtime) < 6 THEN '1-6 hours'
--         WHEN DATE_DIFF('hour', ie.intime, ie.outtime) < 24 THEN '6-24 hours'
--         ELSE '24+ hours'
--     END AS los_category,
--     COUNT(*) AS n_stays_without_hr,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_null_stays
-- FROM icustays ie
-- LEFT JOIN icustay_times it ON ie.icustay_id = it.icustay_id
-- WHERE it.intime_hr IS NULL
-- GROUP BY los_category
-- ORDER BY los_category;

-- Query 4: Validate fuzzy boundaries caught HR measurements
-- Check if HR measurements exist outside strict ICU boundaries
-- SELECT
--     COUNT(*) AS total_stays_with_hr,
--     SUM(CASE WHEN intime_hr < ie.intime THEN 1 ELSE 0 END) AS hr_before_admin_intime,
--     SUM(CASE WHEN outtime_hr > ie.outtime THEN 1 ELSE 0 END) AS hr_after_admin_outtime,
--     ROUND(100.0 * SUM(CASE WHEN intime_hr < ie.intime THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_captured_pre_admission,
--     ROUND(100.0 * SUM(CASE WHEN outtime_hr > ie.outtime THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_captured_post_discharge
-- FROM icustays ie
-- INNER JOIN icustay_times it ON ie.icustay_id = it.icustay_id
-- WHERE it.intime_hr IS NOT NULL;

-- Query 5: Length of stay comparison (administrative vs clinical)
-- SELECT
--     ROUND(AVG(DATE_DIFF('day', ie.intime, ie.outtime)), 2) AS avg_los_admin_days,
--     ROUND(AVG(DATE_DIFF('day', it.intime_hr, it.outtime_hr)), 2) AS avg_los_clinical_days,
--     ROUND(AVG(DATE_DIFF('day', ie.intime, ie.outtime)) -
--           AVG(DATE_DIFF('day', it.intime_hr, it.outtime_hr)), 2) AS avg_difference_days
-- FROM icustays ie
-- INNER JOIN icustay_times it ON ie.icustay_id = it.icustay_id
-- WHERE it.intime_hr IS NOT NULL;


-- =====================================================================
-- NULL HANDLING GUIDANCE FOR DOWNSTREAM QUERIES
-- =====================================================================

-- When using icustay_times in other queries, handle NULLs appropriately:

-- APPROACH 1: INNER JOIN (excludes NULLs - recommended for time-series)
-- SELECT ...
-- FROM icustay_times it
-- INNER JOIN chartevents ce ON it.icustay_id = ce.icustay_id
-- WHERE it.intime_hr IS NOT NULL  -- Explicit NULL check
-- Loses 3.57% of ICU stays, but ensures data quality

-- APPROACH 2: LEFT JOIN with COALESCE (fallback to administrative times)
-- SELECT
--     it.icustay_id,
--     COALESCE(it.intime_hr, ie.intime) AS effective_intime,
--     COALESCE(it.outtime_hr, ie.outtime) AS effective_outtime
-- FROM icustays ie
-- LEFT JOIN icustay_times it ON ie.icustay_id = it.icustay_id
-- Keeps all ICU stays, uses administrative times as fallback

-- APPROACH 3: Filter out NULLs in WHERE clause (same as INNER JOIN)
-- SELECT ...
-- FROM icustay_times it
-- WHERE it.intime_hr IS NOT NULL AND it.outtime_hr IS NOT NULL

-- Recommendation: Use APPROACH 1 (INNER JOIN) for clinical accuracy
-- The 3.57% lost are likely unusable for time-series analysis anyway
