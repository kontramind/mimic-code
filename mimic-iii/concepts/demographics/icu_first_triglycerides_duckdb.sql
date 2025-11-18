-- ===============================================================================
-- MIMIC-III DuckDB: Closest Triglycerides per ICU Stay
-- ===============================================================================
-- This query creates a table with the closest triglycerides measurement to
-- ICU admission for each ICU stay in the MIMIC-III database, optimized for DuckDB.
--
-- The table includes:
--   - Closest triglycerides value (mg/dL)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source)
--   - Minutes from ICU admission to measurement (can be negative if before)
--
-- ITEMID Configuration:
--   To modify which ITEMIDs are considered for triglycerides, edit the list
--   in the WHERE clause within the trig_measurements CTE below.
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - No filters by default - includes all ICU stays for maximum flexibility
--   - Tracks ITEMID to identify measurement source
--   - NULL values indicate no triglycerides measurement during hospital stay
--   - Uses labevents table (laboratory values, not vital signs)
--   - Part of lipid panel series (see also: Total Chol, HDL, LDL tables)
--
-- Unit of Analysis: ICU stays (icustay_id)
--   - Each ICU stay is analyzed independently
--   - One patient can have multiple hospital admissions
--   - One hospital admission can have multiple ICU stays
--   - We find the "closest" measurement to EACH ICU stay's admission time
--
-- DECISION: Time Window from "intime - 7 days" to "outtime" (ICU Stay Bounded)
--   Triglycerides is a SPARSE LAB, measured infrequently as part of lipid panel.
--   We use a bounded window specific to each ICU stay:
--
--   Rationale:
--   - We want the ICU admission baseline (or recent preceding value)
--   - Start: intime - 7 days captures recent measurements (ED, floor, outpatient)
--   - End: outtime ensures we only capture labs from THIS ICU stay, preventing contamination
--   - Without outtime bound, joining on subject_id could capture labs from future ICU stays
--   - This provides clean temporal boundaries for each ICU episode
--   - 7 days is clinically appropriate (lipids relatively stable over days)
--
--   Implementation:
--   - Join on subject_id (patient level - captures all measurements)
--   - Order by absolute distance from intime (closest = smallest |time difference|)
--   - Bounded to [intime - 7d, outtime] to prevent inter-stay contamination
--   - Time offset exposed in output (can be negative if measured before ICU)
--
-- CLINICAL CONTEXT:
--   Triglycerides are a type of fat (lipid) found in blood. High levels increase
--   risk of cardiovascular disease and can cause acute pancreatitis if very high.
--
--   Part of standard lipid panel but NOT routinely measured in ICU.
--
--   Typical use cases in ICU:
--   - Acute pancreatitis workup (severe hypertriglyceridemia is a cause)
--   - Post-cardiac event monitoring
--   - Cardiovascular risk assessment
--   - Pre-existing dyslipidemia management
--   - TPN (Total Parenteral Nutrition) monitoring
--
--   Note: Triglycerides are highly affected by:
--   - Fasting status (should be measured fasting for accuracy)
--   - Critical illness (often elevated during stress response)
--   - Recent food intake (can increase dramatically after meals)
--   - Alcohol consumption
--   - Certain medications (propofol infusions in ICU can elevate levels)
--
--   Important: Calculated LDL (Friedewald equation) is invalid when TG >400 mg/dL.
--
--   Reference Ranges:
--   - Normal: <150 mg/dL
--   - Borderline high: 150-199 mg/dL
--   - High: 200-499 mg/dL
--   - Very high: ≥500 mg/dL
--   - Pancreatitis risk: Typically >1000 mg/dL (severe hypertriglyceridemia)
-- ===============================================================================

DROP TABLE IF EXISTS icu_first_triglycerides;

CREATE TABLE icu_first_triglycerides AS
WITH trig_measurements AS (
    -- Extract all triglycerides measurements, ordered by distance from ICU admission
    SELECT
        ie.icustay_id,
        le.charttime,
        le.itemid,
        le.valuenum,
        -- Calculate time difference from ICU admission (can be negative if before)
        DATE_DIFF('second', ie.intime, le.charttime) AS seconds_from_intime,
        ROW_NUMBER() OVER (
            PARTITION BY ie.icustay_id    -- Each ICU stay analyzed independently
            ORDER BY ABS(DATE_DIFF('second', ie.intime, le.charttime))  -- Closest by absolute distance
        ) AS rn
    FROM icustays ie
    INNER JOIN labevents le
        ON ie.subject_id = le.subject_id  -- Join at PATIENT level (all measurements)
    WHERE le.itemid IN (
        -- =====================================================================
        -- EDIT THIS LIST to configure which ITEMIDs to include
        -- =====================================================================
        51000       -- Triglycerides (mg/dL) - ~266k measurements
        -- =====================================================================
    )
    -- =========================================================================
    -- VALUE RANGE FILTERS - Edit these thresholds as needed
    -- =========================================================================
    AND le.valuenum > 0         -- Lower limit: lab values must be positive
    AND le.valuenum <= 2000     -- Upper limit: allows severe hypertriglyceridemia (mg/dL)
    -- Normal range: <150 mg/dL
    -- Pancreatitis risk: >1000 mg/dL (severe hypertriglyceridemia)
    -- Values >2000 mg/dL are extremely high but possible; >3000 extremely rare
    -- =========================================================================

    -- =========================================================================
    -- TIME WINDOW - Edit to adjust temporal filtering
    -- =========================================================================
    -- IMPORTANT: Bounded to specific ICU stay to prevent contamination
    AND le.charttime >= ie.intime - INTERVAL '7' DAY  -- Start: 7 days before ICU admission
    AND le.charttime <= ie.outtime                    -- End: ICU discharge (prevents future stay contamination)
    -- This ensures we capture:
    --   1. Recent pre-ICU measurements (ED, floor, outpatient within 7 days)
    --   2. Measurements during ICU stay
    --   3. ONLY from THIS specific ICU stay (no contamination from future stays)
    -- =========================================================================
)
SELECT
    ie.icustay_id,
    ie.subject_id,
    ie.hadm_id,
    ie.intime AS icu_intime,

    -- Closest triglycerides measurement (within hospital admission)
    tm.valuenum AS triglycerides_first,
    tm.charttime AS triglycerides_first_charttime,
    tm.itemid AS triglycerides_first_itemid,

    -- Time from ICU admission to measurement (in minutes)
    -- IMPORTANT: Can be NEGATIVE (measured before ICU) or POSITIVE (measured after ICU)
    -- This is different from routine labs which are typically within ±6 hours
    tm.seconds_from_intime / 60.0 AS triglycerides_first_minutes_from_intime

FROM icustays ie
LEFT JOIN trig_measurements tm
    ON ie.icustay_id = tm.icustay_id
    AND tm.rn = 1  -- Only the closest measurement (by absolute time distance)
ORDER BY ie.icustay_id;


-- ===============================================================================
-- DIAGNOSTIC QUERIES
-- ===============================================================================
-- These queries help assess data completeness and quality.
-- Uncomment and run as needed for data validation.

-- Check data completeness: How many ICU stays have triglycerides measurements?
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     SUM(CASE WHEN triglycerides_first IS NOT NULL THEN 1 ELSE 0 END) AS stays_with_trig,
--     ROUND(100.0 * SUM(CASE WHEN triglycerides_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_trig
-- FROM icu_first_triglycerides;

-- Distribution of triglycerides values
-- SELECT
--     MIN(triglycerides_first) AS min_trig,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY triglycerides_first) AS p25_trig,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY triglycerides_first) AS median_trig,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY triglycerides_first) AS p75_trig,
--     MAX(triglycerides_first) AS max_trig
-- FROM icu_first_triglycerides
-- WHERE triglycerides_first IS NOT NULL;

-- Check ITEMID distribution (should only be 51000 unless ITEMIDs are modified)
-- SELECT
--     triglycerides_first_itemid,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_triglycerides
-- WHERE triglycerides_first_itemid IS NOT NULL
-- GROUP BY triglycerides_first_itemid
-- ORDER BY n_stays DESC;

-- Timing analysis: When are closest triglycerides measurements typically taken?
-- Note: This shows the temporal distribution relative to ICU admission
-- Negative values = measured BEFORE ICU admission
-- SELECT
--     CASE
--         WHEN triglycerides_first_minutes_from_intime < -4320 THEN 'More than 3 days before ICU'
--         WHEN triglycerides_first_minutes_from_intime < -1440 THEN '1-3 days before ICU'
--         WHEN triglycerides_first_minutes_from_intime < -360 THEN '6-24 hours before ICU'
--         WHEN triglycerides_first_minutes_from_intime < 0 THEN 'Within 6h before ICU'
--         WHEN triglycerides_first_minutes_from_intime <= 360 THEN 'Within 6h after ICU'
--         WHEN triglycerides_first_minutes_from_intime <= 1440 THEN '6-24 hours after ICU'
--         WHEN triglycerides_first_minutes_from_intime <= 4320 THEN '1-3 days after ICU'
--         ELSE 'More than 3 days after ICU'
--     END AS timing_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_triglycerides
-- WHERE triglycerides_first IS NOT NULL
-- GROUP BY timing_category
-- ORDER BY MIN(triglycerides_first_minutes_from_intime);

-- Check triglycerides level categories (cardiovascular risk & pancreatitis screening)
-- SELECT
--     CASE
--         WHEN triglycerides_first < 150 THEN 'Normal (< 150)'
--         WHEN triglycerides_first < 200 THEN 'Borderline High (150-199)'
--         WHEN triglycerides_first < 500 THEN 'High (200-499)'
--         WHEN triglycerides_first < 1000 THEN 'Very High (500-999)'
--         ELSE 'Severe (≥ 1000) - Pancreatitis Risk'
--     END AS trig_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_triglycerides
-- WHERE triglycerides_first IS NOT NULL
-- GROUP BY trig_category
-- ORDER BY MIN(triglycerides_first);

-- Check for cases where calculated LDL would be invalid (TG >400 mg/dL)
-- This is clinically relevant as it indicates when measured LDL should be used instead
-- SELECT
--     SUM(CASE WHEN triglycerides_first > 400 THEN 1 ELSE 0 END) AS stays_trig_over_400,
--     SUM(CASE WHEN triglycerides_first IS NOT NULL THEN 1 ELSE 0 END) AS stays_with_trig,
--     ROUND(100.0 * SUM(CASE WHEN triglycerides_first > 400 THEN 1 ELSE 0 END) /
--           SUM(CASE WHEN triglycerides_first IS NOT NULL THEN 1 ELSE 0 END), 2) AS pct_invalid_friedewald
-- FROM icu_first_triglycerides;
