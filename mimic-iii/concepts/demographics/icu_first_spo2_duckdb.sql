-- ===============================================================================
-- MIMIC-III DuckDB: First SpO2 (Oxygen Saturation) per ICU Stay
-- ===============================================================================
-- This query creates a table with the first SpO2 measurement for each
-- ICU stay in the MIMIC-III database, optimized for DuckDB.
--
-- The table includes:
--   - First SpO2 value (%)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source/device)
--   - Minutes from ICU admission to measurement
--
-- ITEMID Configuration:
--   To modify which ITEMIDs are considered for SpO2, edit the list
--   in the WHERE clause within the spo2_measurements CTE below.
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - No filters by default - includes all ICU stays for maximum flexibility
--   - Tracks ITEMID to identify measurement source
--   - NULL values indicate no SpO2 measurement during ICU stay
--   - Filters out error measurements (ce.error IS DISTINCT FROM 1)
--
-- Unit of Analysis: ICU stays (icustay_id)
--
-- CLINICAL CONTEXT:
--   SpO2 (peripheral oxygen saturation) measures the percentage of hemoglobin
--   binding sites occupied by oxygen in arterial blood. It is measured non-invasively
--   using pulse oximetry and is one of the most frequently monitored vital signs in ICU.
--
--   SpO2 is ROUTINELY and CONTINUOUSLY monitored in ICU settings.
--
--   Normal Range: 95-100% (in healthy individuals at sea level)
--   Target Range in ICU: Typically 92-96% (varies by condition)
--
--   Clinical Significance:
--   - ≥95%: Normal oxygenation
--   - 90-94%: Mild hypoxemia (may be acceptable in some patients)
--   - 85-89%: Moderate hypoxemia (requires intervention)
--   - <85%: Severe hypoxemia (urgent intervention needed)
--
--   Note: SpO2 accuracy decreases below 80% and can be affected by:
--   - Poor perfusion (shock, hypothermia, vasoconstriction)
--   - Motion artifact
--   - Nail polish or artificial nails
--   - Carbon monoxide poisoning (falsely elevated)
--   - Severe anemia
--   - Dark skin pigmentation (may underestimate)
-- ===============================================================================

DROP TABLE IF EXISTS icu_first_spo2;

CREATE TABLE icu_first_spo2 AS
WITH spo2_measurements AS (
    -- Extract all SpO2 measurements with temporal ordering per ICU stay
    SELECT
        ie.icustay_id,
        ce.charttime,
        ce.itemid,
        ce.valuenum,
        ROW_NUMBER() OVER (PARTITION BY ie.icustay_id ORDER BY ce.charttime) AS rn
    FROM icustays ie
    INNER JOIN chartevents ce
        ON ie.icustay_id = ce.icustay_id
    WHERE ce.itemid IN (
        -- =====================================================================
        -- EDIT THIS LIST to configure which ITEMIDs to include
        -- =====================================================================
        646,        -- SpO2 (CareVue)
        220277      -- SpO2 (MetaVision)
        -- =====================================================================
    )
    -- =========================================================================
    -- VALUE RANGE FILTERS - Edit these thresholds as needed
    -- =========================================================================
    -- Value range filter: physiologically plausible SpO2 values (0-100%)
    -- Filters out data entry errors and artifacts
    AND ce.valuenum > 0
    AND ce.valuenum <= 100
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

    -- First SpO2 measurement
    spo2.valuenum AS spo2_first,
    spo2.charttime AS spo2_first_charttime,
    spo2.itemid AS spo2_first_itemid,

    -- Time from ICU admission to measurement (in minutes)
    DATE_DIFF('second', ie.intime, spo2.charttime) / 60.0 AS spo2_first_minutes_from_intime

FROM icustays ie
LEFT JOIN spo2_measurements spo2
    ON ie.icustay_id = spo2.icustay_id
    AND spo2.rn = 1  -- Only the first measurement
ORDER BY ie.icustay_id;


-- ===============================================================================
-- DIAGNOSTIC QUERIES
-- ===============================================================================
-- These queries help assess data completeness and quality.
-- Uncomment and run as needed for data validation.

-- Check data completeness: How many ICU stays have SpO2 measurements?
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     SUM(CASE WHEN spo2_first IS NOT NULL THEN 1 ELSE 0 END) AS stays_with_spo2,
--     ROUND(100.0 * SUM(CASE WHEN spo2_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_spo2
-- FROM icu_first_spo2;

-- Distribution of SpO2 values
-- SELECT
--     MIN(spo2_first) AS min_spo2,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY spo2_first) AS p25_spo2,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY spo2_first) AS median_spo2,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY spo2_first) AS p75_spo2,
--     MAX(spo2_first) AS max_spo2
-- FROM icu_first_spo2
-- WHERE spo2_first IS NOT NULL;

-- Check ITEMID distribution: Which ITEMIDs are most common?
-- SELECT
--     spo2_first_itemid,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_spo2
-- WHERE spo2_first_itemid IS NOT NULL
-- GROUP BY spo2_first_itemid
-- ORDER BY n_stays DESC;

-- Timing analysis: When are first SpO2 measurements typically taken?
-- SELECT
--     CASE
--         WHEN spo2_first_minutes_from_intime < -300 THEN '5-6 hours before ICU'
--         WHEN spo2_first_minutes_from_intime < -240 THEN '4-5 hours before ICU'
--         WHEN spo2_first_minutes_from_intime < -180 THEN '3-4 hours before ICU'
--         WHEN spo2_first_minutes_from_intime < -120 THEN '2-3 hours before ICU'
--         WHEN spo2_first_minutes_from_intime < -60 THEN '1-2 hours before ICU'
--         WHEN spo2_first_minutes_from_intime < 0 THEN 'Within 1h before ICU'
--         WHEN spo2_first_minutes_from_intime <= 60 THEN 'Within 1h after ICU'
--         WHEN spo2_first_minutes_from_intime <= 360 THEN '1-6 hours after ICU'
--         WHEN spo2_first_minutes_from_intime <= 1440 THEN '6-24 hours after ICU'
--         ELSE 'More than 24h after ICU'
--     END AS timing_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_spo2
-- WHERE spo2_first IS NOT NULL
-- GROUP BY timing_category
-- ORDER BY MIN(spo2_first_minutes_from_intime);

-- Check for hypoxemia and oxygenation status
-- SELECT
--     CASE
--         WHEN spo2_first >= 95 THEN 'Normal (≥ 95%)'
--         WHEN spo2_first >= 90 THEN 'Mild Hypoxemia (90-94%)'
--         WHEN spo2_first >= 85 THEN 'Moderate Hypoxemia (85-89%)'
--         ELSE 'Severe Hypoxemia (< 85%)'
--     END AS spo2_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_spo2
-- WHERE spo2_first IS NOT NULL
-- GROUP BY spo2_category
-- ORDER BY MIN(spo2_first) DESC;

-- Analyze measurements captured before vs during ICU stay
-- SELECT
--     CASE
--         WHEN spo2_first_minutes_from_intime < 0 THEN 'Before ICU admission (within -6h window)'
--         ELSE 'During ICU stay'
--     END AS measurement_timing,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
--     ROUND(AVG(spo2_first), 2) AS avg_spo2,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY spo2_first), 2) AS median_spo2
-- FROM icu_first_spo2
-- WHERE spo2_first IS NOT NULL
-- GROUP BY measurement_timing
-- ORDER BY MIN(spo2_first_minutes_from_intime);

-- Check for critically low SpO2 requiring immediate intervention
-- SELECT
--     CASE
--         WHEN spo2_first >= 92 THEN 'Acceptable (≥ 92%) - Typical ICU target range'
--         WHEN spo2_first >= 88 THEN 'Below Target (88-91%) - Monitor closely'
--         WHEN spo2_first >= 85 THEN 'Low (85-87%) - Intervention likely needed'
--         ELSE 'Critical (< 85%) - Urgent intervention required'
--     END AS clinical_action,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_spo2
-- WHERE spo2_first IS NOT NULL
-- GROUP BY clinical_action
-- ORDER BY MIN(spo2_first) DESC;
