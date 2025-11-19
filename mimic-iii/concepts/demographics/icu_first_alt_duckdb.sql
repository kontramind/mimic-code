-- ===============================================================================
-- MIMIC-III DuckDB: First ALT (Alanine Aminotransferase) per ICU Stay
-- ===============================================================================
-- This query creates a table with the first ALT (Alanine Aminotransferase) measurement
-- for each ICU stay in the MIMIC-III database, optimized for DuckDB.
--
-- The table includes:
--   - First ALT value within time window (IU/L or U/L)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source)
--   - Minutes from ICU admission to measurement
--
-- ITEMID Configuration:
--   To modify which ITEMIDs are considered for ALT, edit the list
--   in the WHERE clause within the alt_measurements CTE below.
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - No filters by default - includes all ICU stays for maximum flexibility
--   - Tracks ITEMID to identify measurement source
--   - NULL values indicate no ALT measurement during the defined time window
--   - Uses labevents table (laboratory values, not vital signs)
--   - Part of routine liver enzyme panel (see also: AST, ALP, Bilirubin)
--
-- Unit of Analysis: ICU stays (icustay_id)
--   - Each ICU stay is analyzed independently
--   - One patient can have multiple hospital admissions
--   - One hospital admission can have multiple ICU stays
--   - We find the "first" measurement for EACH ICU stay
--
-- DECISION: Time Window from "intime - 6 hours" to "outtime" (ICU Stay Bounded)
--   Unlike sparse labs (lipid panels) which use "closest within hospital admission"
--   approach, ALT uses a bounded window specific to each ICU stay:
--
--   Rationale:
--   - ALT is a ROUTINE lab, measured frequently in ICU as part of chemistry panels
--   - We want the ICU admission baseline (or immediately preceding value)
--   - Start: intime - 6 hours captures pre-ICU labs (ED, floor) that reflect admission state
--   - End: outtime ensures we only capture labs from THIS ICU stay, preventing contamination
--   - Without outtime bound, joining on subject_id could capture labs from future ICU stays
--   - This provides clean temporal boundaries for each ICU episode
--
--   Implementation:
--   - Join on subject_id (not hadm_id, as ALT can be checked across admissions)
--   - Order by charttime (first chronologically within window)
--   - Bounded to [intime - 6h, outtime] to prevent inter-stay contamination
--   - Time offset exposed in output (minutes from ICU admission)
--
-- CLINICAL CONTEXT:
--   ALT (Alanine Aminotransferase) is a liver enzyme that is primarily found in
--   hepatocytes. When liver cells are damaged, ALT is released into the bloodstream,
--   making it a sensitive marker for hepatocellular injury.
--
--   Part of standard liver function panel and ROUTINELY measured in ICU.
--
--   Typical use cases in ICU:
--   - Liver function assessment and monitoring
--   - Detection of acute liver injury (drug-induced, ischemic, viral hepatitis)
--   - Monitoring hepatotoxic medications (acetaminophen overdose, antibiotics)
--   - Sepsis-associated liver dysfunction
--   - Assessment of shock liver (ischemic hepatitis)
--   - Part of admission workup for critically ill patients
--
--   ALT vs AST:
--   - ALT is more specific for liver injury (primarily found in liver)
--   - AST is also found in heart, muscle, kidney (less liver-specific)
--   - ALT/AST ratio can help distinguish types of liver disease:
--     * Ratio <1: Suggests cirrhosis, chronic hepatitis
--     * Ratio >2: Suggests alcoholic liver disease
--     * Ratio ~1: Acute viral hepatitis
--
--   Note: ALT levels can be affected by:
--   - Acute hepatocellular injury (viral hepatitis, drug toxicity, ischemia)
--   - Chronic liver disease (cirrhosis, fatty liver disease)
--   - Muscle injury (less common than with AST)
--   - Certain medications (statins, antibiotics, anticonvulsants)
--   - Obesity and metabolic syndrome (mild elevations)
--
--   Reference Ranges:
--   - Normal: 7-56 U/L (varies by lab, gender, and age)
--   - Mild elevation: 1-2x upper limit of normal (ULN)
--   - Moderate elevation: 2-10x ULN
--   - Marked elevation: >10x ULN (suggests acute hepatocellular injury)
--   - Massive elevation: >1000 U/L (acetaminophen toxicity, ischemic hepatitis, acute viral hepatitis)
--
--   Clinical Interpretation:
--   - <100 U/L: Generally considered normal to mildly elevated
--   - 100-300 U/L: Moderate elevation (chronic liver disease, medication effect)
--   - 300-1000 U/L: Significant elevation (acute hepatitis, drug toxicity)
--   - >1000 U/L: Severe hepatocellular injury (requires urgent investigation)
-- ===============================================================================

DROP TABLE IF EXISTS icu_first_alt;

CREATE TABLE icu_first_alt AS
WITH alt_measurements AS (
    -- Extract first ALT measurement within ICU stay time window
    SELECT
        ie.icustay_id,
        le.charttime,
        le.itemid,
        le.valuenum,
        -- Calculate time difference from ICU admission
        DATE_DIFF('second', ie.intime, le.charttime) AS seconds_from_intime,
        ROW_NUMBER() OVER (
            PARTITION BY ie.icustay_id    -- Each ICU stay analyzed independently
            ORDER BY le.charttime         -- First chronologically within bounded window
        ) AS rn
    FROM icustays ie
    INNER JOIN labevents le
        ON ie.subject_id = le.subject_id  -- Join at PATIENT level (routine lab approach)
    WHERE le.itemid IN (
        -- =====================================================================
        -- EDIT THIS LIST to configure which ITEMIDs to include
        -- =====================================================================
        50861       -- Alanine Aminotransferase (ALT) - IU/L or U/L
        -- =====================================================================
    )
    -- =========================================================================
    -- VALUE RANGE FILTERS - Edit these thresholds as needed
    -- =========================================================================
    AND le.valuenum > 0         -- Lower limit: lab values must be positive
    AND le.valuenum <= 10000    -- Upper limit: filters extreme outliers (U/L)
    -- Normal range: 7-56 U/L (varies by lab, gender, age)
    -- Mild elevation: <100 U/L
    -- Moderate elevation: 100-300 U/L
    -- Significant elevation: 300-1000 U/L
    -- Severe elevation: >1000 U/L (acute hepatocellular injury)
    -- Values >10000 U/L are extremely rare (measurement errors or severe acute liver failure)
    -- =========================================================================

    -- =========================================================================
    -- TIME WINDOW - Edit to adjust temporal filtering
    -- =========================================================================
    -- IMPORTANT: Bounded to specific ICU stay to prevent contamination
    AND le.charttime >= ie.intime - INTERVAL '6' HOUR  -- Start: 6h before ICU admission
    AND le.charttime <= ie.outtime                     -- End: ICU discharge (prevents future stay contamination)
    -- This ensures we capture:
    --   1. Pre-ICU labs from ED/floor (admission baseline)
    --   2. Labs during ICU stay
    --   3. ONLY from THIS specific ICU stay (no contamination from future stays)
    -- =========================================================================
)
SELECT
    ie.icustay_id,
    ie.subject_id,
    ie.hadm_id,
    ie.intime AS icu_intime,
    ie.outtime AS icu_outtime,

    -- First ALT measurement (within ICU stay bounded window)
    am.valuenum AS alt_first,
    am.charttime AS alt_first_charttime,
    am.itemid AS alt_first_itemid,

    -- Time from ICU admission to measurement (in minutes)
    -- Can be NEGATIVE (measured before ICU admission, within -6h window)
    -- or POSITIVE (measured during ICU stay)
    am.seconds_from_intime / 60.0 AS alt_first_minutes_from_intime

FROM icustays ie
LEFT JOIN alt_measurements am
    ON ie.icustay_id = am.icustay_id
    AND am.rn = 1  -- Only the first measurement (chronologically)
ORDER BY ie.icustay_id;


-- ===============================================================================
-- DIAGNOSTIC QUERIES
-- ===============================================================================
-- These queries help assess data completeness and quality.
-- Uncomment and run as needed for data validation.

-- Check data completeness: How many ICU stays have ALT measurements?
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     SUM(CASE WHEN alt_first IS NOT NULL THEN 1 ELSE 0 END) AS stays_with_alt,
--     ROUND(100.0 * SUM(CASE WHEN alt_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_alt
-- FROM icu_first_alt;

-- Distribution of ALT values
-- SELECT
--     MIN(alt_first) AS min_alt,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY alt_first) AS p25_alt,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY alt_first) AS median_alt,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY alt_first) AS p75_alt,
--     MAX(alt_first) AS max_alt
-- FROM icu_first_alt
-- WHERE alt_first IS NOT NULL;

-- Check ITEMID distribution (should only be 50861 unless ITEMIDs are modified)
-- SELECT
--     alt_first_itemid,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_alt
-- WHERE alt_first_itemid IS NOT NULL
-- GROUP BY alt_first_itemid
-- ORDER BY n_stays DESC;

-- Timing analysis: When are first ALT measurements typically taken relative to ICU admission?
-- SELECT
--     CASE
--         WHEN alt_first_minutes_from_intime < -300 THEN '5-6 hours before ICU'
--         WHEN alt_first_minutes_from_intime < -240 THEN '4-5 hours before ICU'
--         WHEN alt_first_minutes_from_intime < -180 THEN '3-4 hours before ICU'
--         WHEN alt_first_minutes_from_intime < -120 THEN '2-3 hours before ICU'
--         WHEN alt_first_minutes_from_intime < -60 THEN '1-2 hours before ICU'
--         WHEN alt_first_minutes_from_intime < 0 THEN 'Within 1h before ICU'
--         WHEN alt_first_minutes_from_intime <= 60 THEN 'Within 1h after ICU'
--         WHEN alt_first_minutes_from_intime <= 360 THEN '1-6 hours after ICU'
--         WHEN alt_first_minutes_from_intime <= 1440 THEN '6-24 hours after ICU'
--         ELSE 'More than 24h after ICU'
--     END AS timing_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_alt
-- WHERE alt_first IS NOT NULL
-- GROUP BY timing_category
-- ORDER BY MIN(alt_first_minutes_from_intime);

-- Check ALT level categories (liver injury assessment)
-- SELECT
--     CASE
--         WHEN alt_first <= 56 THEN 'Normal (≤ 56)'
--         WHEN alt_first <= 100 THEN 'Mildly Elevated (57-100)'
--         WHEN alt_first <= 300 THEN 'Moderately Elevated (101-300)'
--         WHEN alt_first <= 1000 THEN 'Significantly Elevated (301-1000)'
--         ELSE 'Severely Elevated (> 1000) - Acute Hepatocellular Injury'
--     END AS alt_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_alt
-- WHERE alt_first IS NOT NULL
-- GROUP BY alt_category
-- ORDER BY MIN(alt_first);

-- Check ALT by clinical significance thresholds
-- SELECT
--     CASE
--         WHEN alt_first < 100 THEN 'Normal/Mild (< 100) - Low clinical concern'
--         WHEN alt_first < 300 THEN 'Moderate (100-299) - Monitor, check medications'
--         WHEN alt_first < 1000 THEN 'Significant (300-999) - Investigate cause'
--         ELSE 'Severe (≥ 1000) - Acute injury, urgent workup needed'
--     END AS clinical_significance,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_alt
-- WHERE alt_first IS NOT NULL
-- GROUP BY clinical_significance
-- ORDER BY MIN(alt_first);

-- Analyze measurements captured before vs during ICU stay
-- SELECT
--     CASE
--         WHEN alt_first_minutes_from_intime < 0 THEN 'Before ICU admission (within -6h window)'
--         ELSE 'During ICU stay'
--     END AS measurement_timing,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
--     ROUND(AVG(alt_first), 2) AS avg_alt,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY alt_first), 2) AS median_alt
-- FROM icu_first_alt
-- WHERE alt_first IS NOT NULL
-- GROUP BY measurement_timing
-- ORDER BY MIN(alt_first_minutes_from_intime);
