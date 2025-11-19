-- ===============================================================================
-- MIMIC-III DuckDB: First Albumin per ICU Stay
-- ===============================================================================
-- This query creates a table with the first Albumin measurement
-- for each ICU stay in the MIMIC-III database, optimized for DuckDB.
--
-- The table includes:
--   - First Albumin value within time window (g/dL)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source)
--   - Minutes from ICU admission to measurement
--
-- ITEMID Configuration:
--   To modify which ITEMIDs are considered for Albumin, edit the list
--   in the WHERE clause within the alb_measurements CTE below.
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - No filters by default - includes all ICU stays for maximum flexibility
--   - Tracks ITEMID to identify measurement source
--   - NULL values indicate no Albumin measurement during the defined time window
--   - Uses labevents table (laboratory values, not vital signs)
--   - Part of Chemistry panel, SELECTIVELY ordered (51% ICU coverage)
--
-- Unit of Analysis: ICU stays (icustay_id)
--   - Each ICU stay is analyzed independently
--   - One patient can have multiple hospital admissions
--   - One hospital admission can have multiple ICU stays
--   - We find the "first" measurement for EACH ICU stay
--
-- DECISION: Time Window from "intime - 6 hours" to "outtime" (ICU Stay Bounded)
--   Unlike sparse labs (lipid panels) which use "closest within hospital admission"
--   approach, Albumin uses a bounded window specific to each ICU stay:
--
--   Rationale:
--   - Albumin is SELECTIVELY ordered (51.01% ICU coverage) but REPEATEDLY monitored (6.93 measurements/stay)
--   - Pattern differs from truly sparse labs (lipids: <10% coverage, 1-2 measurements)
--   - We want the ICU admission baseline (or immediately preceding value)
--   - Start: intime - 6 hours captures pre-ICU labs (ED, floor) that reflect admission state
--   - End: outtime ensures we only capture labs from THIS ICU stay, preventing contamination
--   - Without outtime bound, joining on subject_id could capture labs from future ICU stays
--   - This provides clean temporal boundaries for each ICU episode
--
--   Coverage Statistics:
--   - 51.01% of ICU stays have albumin measurements (selective but common)
--   - Average 6.93 measurements per stay when ordered (repeated monitoring)
--   - Median timing: 10.69 hours after ICU admission
--   - 35.06% measured before ICU admission (pre-ICU labs from ED/floor)
--
--   Implementation:
--   - Join on subject_id (not hadm_id, as Albumin can be checked across admissions)
--   - Order by charttime (first chronologically within window)
--   - Bounded to [intime - 6h, outtime] to prevent inter-stay contamination
--   - Time offset exposed in output (minutes from ICU admission)
--
-- CLINICAL CONTEXT:
--   Albumin is the most abundant protein in blood plasma, synthesized by the liver.
--   It serves multiple critical functions: maintaining oncotic pressure (prevents edema),
--   transporting hormones/drugs/fatty acids, and acting as a biomarker for nutritional
--   status, liver function, and chronic illness severity.
--
--   Part of Chemistry panel, SELECTIVELY ordered when clinical concerns exist.
--
--   Typical use cases in ICU:
--   - Nutritional status assessment (malnutrition screening)
--   - Liver function evaluation (synthetic function marker)
--   - Severity of illness indicator (APACHE III scoring component)
--   - Fluid management planning (low albumin → edema risk)
--   - Prognostication (low albumin associated with worse outcomes)
--   - Nephrotic syndrome diagnosis (proteinuria workup)
--   - Ascites evaluation (SAAG calculation: serum albumin - ascites albumin)
--   - Chronic disease monitoring (inflammation, critical illness)
--
--   Albumin Measurement:
--   - itemid 50862: Albumin (Blood/Serum/Plasma) - Standard measurement
--   - Units: g/dL (grams per deciliter)
--   - Part of comprehensive metabolic panel (CMP) or ordered separately
--
--   Note: Albumin levels can be affected by:
--   - Liver disease (cirrhosis, hepatitis) - DECREASED synthesis
--   - Malnutrition/cachexia - DECREASED synthesis
--   - Chronic inflammation/critical illness - DECREASED (negative acute phase protein)
--   - Protein-losing conditions:
--     * Nephrotic syndrome (urinary losses)
--     * Protein-losing enteropathy (GI losses)
--     * Burns, exudative wounds (external losses)
--   - Fluid status:
--     * Dehydration/volume depletion - FALSELY ELEVATED (hemoconcentration)
--     * Fluid overload/resuscitation - FALSELY LOWERED (hemodilution)
--   - Capillary leak syndromes (sepsis, ARDS) - redistribution to interstitial space
--   - Congestive heart failure - dilutional hypoalbuminemia
--
--   Reference Ranges:
--   - Normal: 3.5-5.5 g/dL
--   - Note: "Normal" albumin doesn't exclude malnutrition or liver disease
--
--   Clinical Interpretation:
--   - Severe Hypoalbuminemia: <2.0 g/dL (high mortality risk, severe malnutrition/liver disease)
--   - Moderate Hypoalbuminemia: 2.0-2.9 g/dL (significant nutritional/liver concerns)
--   - Mild Hypoalbuminemia: 3.0-3.4 g/dL (mild malnutrition, inflammation, or dilution)
--   - Normal: 3.5-5.5 g/dL
--   - Hyperalbuminemia: >5.5 g/dL (rare, usually dehydration/hemoconcentration)
--
--   APACHE III Scoring (Severity Assessment):
--   Albumin is a component of APACHE III severity score with these thresholds:
--   - <2.0 g/dL: 11 points (highest severity)
--   - 2.0-2.4 g/dL: 6 points
--   - 2.5-4.4 g/dL: 0 points (reference)
--   - ≥4.5 g/dL: 4 points (deviation from normal reference ~3.5)
--   (See: mimic-iv/concepts/score/apsiii.sql for implementation)
--
--   Prognostic Value:
--   - Low albumin (<3.0 g/dL) strongly associated with:
--     * Increased ICU mortality
--     * Longer hospital stays
--     * Higher readmission rates
--     * Increased infection risk
--     * Poor wound healing
--   - Each 1 g/dL decrease in albumin associated with ~137% increase in mortality odds
--
--   Important Limitations:
--   - NOT a sensitive marker of acute malnutrition (long half-life: 20 days)
--   - Decreased in acute inflammation (negative acute phase reactant) even with adequate nutrition
--   - Affected by hydration status (not purely nutritional marker)
--   - Better for chronic assessment than acute changes
--   - Prealbumin (transthyretin) is better for short-term nutritional monitoring
--
--   Related Measures:
--   - Globulin (itemid 50930): Other serum proteins
--   - Total Protein (itemid 50976): Albumin + Globulin
--   - Albumin/Globulin Ratio: Diagnostic for certain conditions
--   - Prealbumin: More sensitive short-term nutritional marker
--
--   Critical Values (require immediate attention):
--   - <1.5 g/dL: Severe hypoalbuminemia, very high mortality risk
--   - >6.0 g/dL: Severe dehydration or laboratory error
-- ===============================================================================

DROP TABLE IF EXISTS icu_first_albumin;

CREATE TABLE icu_first_albumin AS
WITH alb_measurements AS (
    -- Extract first Albumin measurement within ICU stay time window
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
        ON ie.subject_id = le.subject_id  -- Join at PATIENT level (selective lab, repeated monitoring approach)
    WHERE le.itemid IN (
        -- =====================================================================
        -- EDIT THIS LIST to configure which ITEMIDs to include
        -- =====================================================================
        50862       -- Albumin (Blood/Serum/Plasma) - Standard measurement (~775k in MIMIC-IV, ~147k in MIMIC-III)
        -- Note: itemid 50862 is serum/plasma albumin from labevents
        -- Other albumin ITEMIDs exist for different fluids (ascites, pleural, urine) - NOT included here
        -- =====================================================================
    )
    -- =========================================================================
    -- VALUE RANGE FILTERS - Edit these thresholds as needed
    -- =========================================================================
    AND le.valuenum > 0         -- Lower limit: lab values must be positive
    AND le.valuenum <= 10       -- Upper limit: filters extreme outliers (g/dL)
    -- Normal range: 3.5-5.5 g/dL
    -- Severe hypoalbuminemia: <2.0 g/dL
    -- Mild hypoalbuminemia: 3.0-3.4 g/dL
    -- Values >10 g/dL are physiologically impossible (measurement errors)
    -- Validation threshold matches existing codebase (chemistry.sql, pivoted_lab.sql)
    -- =========================================================================

    -- =========================================================================
    -- TIME WINDOW - Edit to adjust temporal filtering
    -- =========================================================================
    -- IMPORTANT: Bounded to specific ICU stay to prevent contamination
    AND le.charttime >= ie.intime - INTERVAL '6' HOUR  -- Start: 6h before ICU admission
    AND le.charttime <= ie.outtime                     -- End: ICU discharge (prevents future stay contamination)
    -- This ensures we capture:
    --   1. Pre-ICU labs from ED/floor (admission baseline) - 35% of measurements
    --   2. Labs during ICU stay - median 10.69 hours after admission
    --   3. ONLY from THIS specific ICU stay (no contamination from future stays)
    -- =========================================================================
)
SELECT
    ie.icustay_id,
    ie.subject_id,
    ie.hadm_id,
    ie.intime AS icu_intime,
    ie.outtime AS icu_outtime,

    -- First Albumin measurement (within ICU stay bounded window)
    am.valuenum AS albumin_first,
    am.charttime AS albumin_first_charttime,
    am.itemid AS albumin_first_itemid,

    -- Time from ICU admission to measurement (in minutes)
    -- Can be NEGATIVE (measured before ICU admission, within -6h window)
    -- or POSITIVE (measured during ICU stay)
    am.seconds_from_intime / 60.0 AS albumin_first_minutes_from_intime

FROM icustays ie
LEFT JOIN alb_measurements am
    ON ie.icustay_id = am.icustay_id
    AND am.rn = 1  -- Only the first measurement (chronologically)
ORDER BY ie.icustay_id;


-- ===============================================================================
-- DIAGNOSTIC QUERIES
-- ===============================================================================
-- These queries help assess data completeness and quality.
-- Uncomment and run as needed for data validation.

-- Check data completeness: How many ICU stays have Albumin measurements?
-- Expected: ~51% coverage (selective ordering, not routine like CBC)
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     SUM(CASE WHEN albumin_first IS NOT NULL THEN 1 ELSE 0 END) AS stays_with_albumin,
--     ROUND(100.0 * SUM(CASE WHEN albumin_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_albumin
-- FROM icu_first_albumin;

-- Distribution of Albumin values
-- SELECT
--     MIN(albumin_first) AS min_albumin,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY albumin_first) AS p25_albumin,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY albumin_first) AS median_albumin,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY albumin_first) AS p75_albumin,
--     MAX(albumin_first) AS max_albumin,
--     ROUND(AVG(albumin_first), 2) AS mean_albumin
-- FROM icu_first_albumin
-- WHERE albumin_first IS NOT NULL;

-- Timing analysis: When are first Albumin measurements typically taken relative to ICU admission?
-- Expected: Median ~10.69 hours, 35% before ICU admission
-- SELECT
--     CASE
--         WHEN albumin_first_minutes_from_intime < -300 THEN '5-6 hours before ICU'
--         WHEN albumin_first_minutes_from_intime < -240 THEN '4-5 hours before ICU'
--         WHEN albumin_first_minutes_from_intime < -180 THEN '3-4 hours before ICU'
--         WHEN albumin_first_minutes_from_intime < -120 THEN '2-3 hours before ICU'
--         WHEN albumin_first_minutes_from_intime < -60 THEN '1-2 hours before ICU'
--         WHEN albumin_first_minutes_from_intime < 0 THEN 'Within 1h before ICU'
--         WHEN albumin_first_minutes_from_intime <= 60 THEN 'Within 1h after ICU'
--         WHEN albumin_first_minutes_from_intime <= 360 THEN '1-6 hours after ICU'
--         WHEN albumin_first_minutes_from_intime <= 1440 THEN '6-24 hours after ICU'
--         ELSE 'More than 24h after ICU'
--     END AS timing_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_albumin
-- WHERE albumin_first IS NOT NULL
-- GROUP BY timing_category
-- ORDER BY MIN(albumin_first_minutes_from_intime);

-- Check Albumin level categories (nutritional/severity assessment)
-- SELECT
--     CASE
--         WHEN albumin_first < 1.5 THEN 'Critical (<1.5) - Very high mortality risk'
--         WHEN albumin_first < 2.0 THEN 'Severe (1.5-1.9) - High mortality risk'
--         WHEN albumin_first < 2.5 THEN 'Moderate-Severe (2.0-2.4)'
--         WHEN albumin_first < 3.0 THEN 'Moderate (2.5-2.9)'
--         WHEN albumin_first < 3.5 THEN 'Mild (3.0-3.4)'
--         WHEN albumin_first <= 5.5 THEN 'Normal (3.5-5.5)'
--         ELSE 'Elevated (>5.5) - Likely dehydration'
--     END AS albumin_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_albumin
-- WHERE albumin_first IS NOT NULL
-- GROUP BY albumin_category
-- ORDER BY MIN(albumin_first);

-- APACHE III scoring categories (for severity assessment)
-- SELECT
--     CASE
--         WHEN albumin_first < 2.0 THEN '<2.0 (11 points - highest severity)'
--         WHEN albumin_first < 2.5 THEN '2.0-2.4 (6 points)'
--         WHEN albumin_first < 4.5 THEN '2.5-4.4 (0 points - reference)'
--         ELSE '≥4.5 (4 points - deviation from normal)'
--     END AS apache_iii_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
--     ROUND(AVG(albumin_first), 2) AS avg_albumin
-- FROM icu_first_albumin
-- WHERE albumin_first IS NOT NULL
-- GROUP BY apache_iii_category
-- ORDER BY MIN(albumin_first);

-- Mortality risk stratification by albumin
-- SELECT
--     CASE
--         WHEN albumin_first < 2.0 THEN 'Very High Risk (<2.0)'
--         WHEN albumin_first < 2.5 THEN 'High Risk (2.0-2.4)'
--         WHEN albumin_first < 3.0 THEN 'Moderate Risk (2.5-2.9)'
--         WHEN albumin_first < 3.5 THEN 'Low-Moderate Risk (3.0-3.4)'
--         ELSE 'Low Risk (≥3.5)'
--     END AS risk_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_albumin
-- WHERE albumin_first IS NOT NULL
-- GROUP BY risk_category
-- ORDER BY MIN(albumin_first);

-- Analyze measurements captured before vs during ICU stay
-- SELECT
--     CASE
--         WHEN albumin_first_minutes_from_intime < 0 THEN 'Before ICU admission (within -6h window)'
--         ELSE 'During ICU stay'
--     END AS measurement_timing,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
--     ROUND(AVG(albumin_first), 2) AS avg_albumin,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY albumin_first), 2) AS median_albumin
-- FROM icu_first_albumin
-- WHERE albumin_first IS NOT NULL
-- GROUP BY measurement_timing
-- ORDER BY MIN(albumin_first_minutes_from_intime);

-- Check for hypoalbuminemia prevalence (clinical significance)
-- SELECT
--     CASE
--         WHEN albumin_first < 3.0 THEN 'Hypoalbuminemia (<3.0)'
--         WHEN albumin_first < 3.5 THEN 'Low-normal (3.0-3.4)'
--         ELSE 'Normal (≥3.5)'
--     END AS albumin_status,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_albumin
-- WHERE albumin_first IS NOT NULL
-- GROUP BY albumin_status
-- ORDER BY MIN(albumin_first);

-- ===============================================================================
-- USAGE EXAMPLES
-- ===============================================================================

-- Example 1: Join with readmission table for albumin-readmission analysis
-- SELECT
--     ir.icustay_id,
--     ir.leads_to_readmission_30d,
--     alb.albumin_first,
--     CASE
--         WHEN alb.albumin_first < 2.5 THEN 'Low (<2.5)'
--         WHEN alb.albumin_first < 3.5 THEN 'Borderline (2.5-3.4)'
--         ELSE 'Normal (≥3.5)'
--     END AS albumin_category
-- FROM icu_readmission_30d ir
-- LEFT JOIN icu_first_albumin alb ON ir.icustay_id = alb.icustay_id
-- WHERE ir.is_last_icu_stay = 0;

-- Example 2: Combine with other first labs for comprehensive baseline assessment
-- SELECT
--     ie.icustay_id,
--     alb.albumin_first,
--     hgb.hemoglobin_first,
--     wbc.wbc_first,
--     plt.platelet_first
-- FROM icustays ie
-- LEFT JOIN icu_first_albumin alb ON ie.icustay_id = alb.icustay_id
-- LEFT JOIN icu_first_hemoglobin hgb ON ie.icustay_id = hgb.icustay_id
-- LEFT JOIN icu_first_wbc wbc ON ie.icustay_id = wbc.icustay_id
-- LEFT JOIN icu_first_platelet plt ON ie.icustay_id = plt.icustay_id;
