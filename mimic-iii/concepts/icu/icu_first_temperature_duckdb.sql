-- ===============================================================================
-- MIMIC-III DuckDB: First Temperature per ICU Stay
-- ===============================================================================
-- This query creates a table with the first body temperature measurement for each
-- ICU stay in the MIMIC-III database, optimized for DuckDB.
--
-- The table includes:
--   - First temperature value (°C - Celsius)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source/device)
--   - Minutes from ICU admission to measurement
--
-- ITEMID Configuration:
--   To modify which ITEMIDs are considered for temperature, edit the list
--   in the WHERE clause within the temp_measurements CTE below.
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - No filters by default - includes all ICU stays for maximum flexibility
--   - Tracks ITEMID to identify measurement source
--   - NULL values indicate no temperature measurement during ICU stay
--   - Filters out error measurements (ce.error IS DISTINCT FROM 1)
--   - Automatically converts Fahrenheit to Celsius for consistency
--   - All output values are in Celsius (°C)
--
-- Unit of Analysis: ICU stays (icustay_id)
--
-- CLINICAL CONTEXT:
--   Body temperature is a core vital sign that reflects the body's ability to
--   generate and dissipate heat. Temperature is ROUTINELY and CONTINUOUSLY
--   monitored in ICU settings as part of standard vital sign assessment.
--
--   Normal Range: 36.1-37.2°C (97.0-99.0°F)
--   Core temperature: Typically 0.5°C higher than peripheral measurement
--
--   Clinical Significance:
--   - Hypothermia: <35°C (mild: 32-35°C, moderate: 28-32°C, severe: <28°C)
--   - Normal: 36.1-37.2°C
--   - Fever (Pyrexia): >37.2°C (low-grade: 37.2-38.3°C, high: >38.3°C)
--   - Hyperpyrexia: >41°C (medical emergency)
--
--   Temperature Abnormalities in ICU:
--   - Fever: Common in infection, inflammation, drug reactions
--   - Hypothermia: Sepsis, exposure, post-cardiac arrest, therapeutic cooling
--   - Temperature instability: Sign of autonomic dysfunction, brainstem injury
--
--   Measurement Sites (affects normal range):
--   - Oral: Most common, 0.5°C lower than core
--   - Rectal: Closest to core temperature
--   - Axillary (armpit): 0.5-1°C lower than oral
--   - Tympanic (ear): Close to core temperature
--   - Temporal artery: Non-invasive, approximates core
--   - Bladder/esophageal: Core temperature in critical care
--
--   Note: Temperature can be affected by:
--   - Time of day (lowest in early morning, highest in late afternoon)
--   - Age (elderly may have lower baseline, blunted fever response)
--   - Recent physical activity or bathing
--   - Medications (antipyretics, anesthetics, sedatives)
--   - Environmental factors (ICU temperature, warming/cooling devices)
--   - Measurement site and technique
-- ===============================================================================

DROP TABLE IF EXISTS icu_first_temperature;

CREATE TABLE icu_first_temperature AS
WITH temp_measurements AS (
    -- Extract all temperature measurements with temporal ordering per ICU stay
    SELECT
        ie.icustay_id,
        ce.charttime,
        ce.itemid,
        -- Convert all temperatures to Celsius for consistency
        CASE
            WHEN ce.itemid IN (223761, 678) THEN (ce.valuenum - 32) / 1.8  -- Fahrenheit to Celsius
            WHEN ce.itemid IN (223762, 676) THEN ce.valuenum                -- Already Celsius
        END AS valuenum_celsius,
        ROW_NUMBER() OVER (PARTITION BY ie.icustay_id ORDER BY ce.charttime) AS rn
    FROM icustays ie
    INNER JOIN chartevents ce
        ON ie.icustay_id = ce.icustay_id
    WHERE ce.itemid IN (
        -- =====================================================================
        -- EDIT THIS LIST to configure which ITEMIDs to include
        -- =====================================================================
        -- MetaVision System:
        223761,     -- Temperature Fahrenheit
        223762,     -- Temperature Celsius
        -- CareVue System:
        676,        -- Temperature C
        678         -- Temperature F
        -- Note: 677 (Temperature C calc) and 679 (Temperature F calc) are less commonly used
        -- =====================================================================
    )
    -- =========================================================================
    -- VALUE RANGE FILTERS - Edit these thresholds as needed
    -- =========================================================================
    -- Filter by original measurement ranges (before conversion)
    AND (
        (ce.itemid IN (223761, 678) AND ce.valuenum > 70 AND ce.valuenum < 120)  -- Fahrenheit: 70-120°F
        OR
        (ce.itemid IN (223762, 676) AND ce.valuenum > 10 AND ce.valuenum < 50)   -- Celsius: 10-50°C
    )
    -- These ranges filter out obvious data entry errors while allowing:
    -- - Severe hypothermia (therapeutic or pathologic)
    -- - Severe hyperthermia (fever, environmental)
    -- =========================================================================

    -- Exclude measurements marked as errors
    AND ce.error IS DISTINCT FROM 1

    -- =========================================================================
    -- TIME WINDOW - ROUTINE vital sign pattern
    -- =========================================================================
    AND ce.charttime >= ie.intime - INTERVAL '6' HOUR  -- Capture pre-ICU measurements
    AND ce.charttime <= ie.outtime                     -- Bound to ICU stay
    -- =========================================================================
)
SELECT
    ie.icustay_id,
    ie.subject_id,
    ie.hadm_id,
    ie.intime AS icu_intime,
    ie.outtime AS icu_outtime,

    -- First temperature measurement (always in Celsius)
    tm.valuenum_celsius AS temperature_first,
    tm.charttime AS temperature_first_charttime,
    tm.itemid AS temperature_first_itemid,

    -- Time from ICU admission to measurement (in minutes)
    DATE_DIFF('second', ie.intime, tm.charttime) / 60.0 AS temperature_first_minutes_from_intime

FROM icustays ie
LEFT JOIN temp_measurements tm
    ON ie.icustay_id = tm.icustay_id
    AND tm.rn = 1  -- Only the first measurement
ORDER BY ie.icustay_id;


-- ===============================================================================
-- DIAGNOSTIC QUERIES
-- ===============================================================================
-- These queries help assess data completeness and quality.
-- Uncomment and run as needed for data validation.

-- Check data completeness: How many ICU stays have temperature measurements?
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     SUM(CASE WHEN temperature_first IS NOT NULL THEN 1 ELSE 0 END) AS stays_with_temperature,
--     ROUND(100.0 * SUM(CASE WHEN temperature_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_temperature
-- FROM icu_first_temperature;

-- Distribution of temperature values (all in Celsius)
-- SELECT
--     MIN(temperature_first) AS min_temp,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY temperature_first) AS p25_temp,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY temperature_first) AS median_temp,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY temperature_first) AS p75_temp,
--     MAX(temperature_first) AS max_temp
-- FROM icu_first_temperature
-- WHERE temperature_first IS NOT NULL;

-- Check ITEMID distribution: Which ITEMIDs are most common?
-- SELECT
--     temperature_first_itemid,
--     CASE
--         WHEN temperature_first_itemid = 223761 THEN 'Temperature Fahrenheit (MetaVision)'
--         WHEN temperature_first_itemid = 223762 THEN 'Temperature Celsius (MetaVision)'
--         WHEN temperature_first_itemid = 676 THEN 'Temperature C (CareVue)'
--         WHEN temperature_first_itemid = 678 THEN 'Temperature F (CareVue)'
--     END AS itemid_description,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_temperature
-- WHERE temperature_first_itemid IS NOT NULL
-- GROUP BY temperature_first_itemid
-- ORDER BY n_stays DESC;

-- Timing analysis: When are first temperature measurements typically taken?
-- SELECT
--     CASE
--         WHEN temperature_first_minutes_from_intime < -300 THEN '5-6 hours before ICU'
--         WHEN temperature_first_minutes_from_intime < -240 THEN '4-5 hours before ICU'
--         WHEN temperature_first_minutes_from_intime < -180 THEN '3-4 hours before ICU'
--         WHEN temperature_first_minutes_from_intime < -120 THEN '2-3 hours before ICU'
--         WHEN temperature_first_minutes_from_intime < -60 THEN '1-2 hours before ICU'
--         WHEN temperature_first_minutes_from_intime < 0 THEN 'Within 1h before ICU'
--         WHEN temperature_first_minutes_from_intime <= 60 THEN 'Within 1h after ICU'
--         WHEN temperature_first_minutes_from_intime <= 360 THEN '1-6 hours after ICU'
--         WHEN temperature_first_minutes_from_intime <= 1440 THEN '6-24 hours after ICU'
--         ELSE 'More than 24h after ICU'
--     END AS timing_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_temperature
-- WHERE temperature_first IS NOT NULL
-- GROUP BY timing_category
-- ORDER BY MIN(temperature_first_minutes_from_intime);

-- Check for hypothermia and hyperthermia
-- SELECT
--     CASE
--         WHEN temperature_first < 28 THEN 'Severe Hypothermia (< 28°C)'
--         WHEN temperature_first < 32 THEN 'Moderate Hypothermia (28-32°C)'
--         WHEN temperature_first < 35 THEN 'Mild Hypothermia (32-35°C)'
--         WHEN temperature_first <= 37.2 THEN 'Normal (35-37.2°C)'
--         WHEN temperature_first <= 38.3 THEN 'Low-Grade Fever (37.2-38.3°C)'
--         WHEN temperature_first <= 41 THEN 'High Fever (38.3-41°C)'
--         ELSE 'Hyperpyrexia (> 41°C) - Medical Emergency'
--     END AS temperature_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_temperature
-- WHERE temperature_first IS NOT NULL
-- GROUP BY temperature_category
-- ORDER BY MIN(temperature_first);

-- Analyze measurements captured before vs during ICU stay
-- SELECT
--     CASE
--         WHEN temperature_first_minutes_from_intime < 0 THEN 'Before ICU admission (within -6h window)'
--         ELSE 'During ICU stay'
--     END AS measurement_timing,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
--     ROUND(AVG(temperature_first), 2) AS avg_temp,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY temperature_first), 2) AS median_temp
-- FROM icu_first_temperature
-- WHERE temperature_first IS NOT NULL
-- GROUP BY measurement_timing
-- ORDER BY MIN(temperature_first_minutes_from_intime);

-- Check for critically abnormal temperatures requiring intervention
-- SELECT
--     CASE
--         WHEN temperature_first >= 36.1 AND temperature_first <= 37.2 THEN 'Normal (36.1-37.2°C)'
--         WHEN temperature_first < 35 THEN 'Hypothermia (< 35°C) - Warming needed'
--         WHEN temperature_first < 36.1 THEN 'Subnormal (35-36.1°C) - Monitor'
--         WHEN temperature_first <= 38.3 THEN 'Low-Grade Fever (37.2-38.3°C) - Monitor'
--         WHEN temperature_first <= 39 THEN 'Moderate Fever (38.3-39°C) - Consider antipyretics'
--         ELSE 'High Fever/Hyperpyrexia (> 39°C) - Urgent intervention'
--     END AS clinical_action,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_temperature
-- WHERE temperature_first IS NOT NULL
-- GROUP BY clinical_action
-- ORDER BY MIN(temperature_first);

-- Compare MetaVision vs CareVue system temperatures
-- SELECT
--     CASE
--         WHEN temperature_first_itemid IN (223761, 223762) THEN 'MetaVision'
--         WHEN temperature_first_itemid IN (676, 678) THEN 'CareVue'
--     END AS monitoring_system,
--     COUNT(*) AS n_stays,
--     ROUND(AVG(temperature_first), 2) AS avg_temp,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY temperature_first), 2) AS median_temp,
--     ROUND(STDDEV(temperature_first), 2) AS std_temp
-- FROM icu_first_temperature
-- WHERE temperature_first IS NOT NULL
-- GROUP BY monitoring_system;
