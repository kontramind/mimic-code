-- ===============================================================================
-- MIMIC-III DuckDB: First AST (Aspartate Aminotransferase) per ICU Stay
-- ===============================================================================
-- This query creates a table with the first AST (Aspartate Aminotransferase) measurement
-- for each ICU stay in the MIMIC-III database, optimized for DuckDB.
--
-- The table includes:
--   - First AST value within time window (IU/L or U/L)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source)
--   - Minutes from ICU admission to measurement
--
-- ITEMID Configuration:
--   To modify which ITEMIDs are considered for AST, edit the list
--   in the WHERE clause within the ast_measurements CTE below.
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - No filters by default - includes all ICU stays for maximum flexibility
--   - Tracks ITEMID to identify measurement source
--   - NULL values indicate no AST measurement during the defined time window
--   - Uses labevents table (laboratory values, not vital signs)
--   - Part of routine liver enzyme panel (see also: ALT, ALP, Bilirubin)
--
-- Unit of Analysis: ICU stays (icustay_id)
--   - Each ICU stay is analyzed independently
--   - One patient can have multiple hospital admissions
--   - One hospital admission can have multiple ICU stays
--   - We find the "first" measurement for EACH ICU stay
--
-- DECISION: Time Window from "intime - 6 hours" to "outtime" (ICU Stay Bounded)
--   Unlike sparse labs (lipid panels) which use "closest within hospital admission"
--   approach, AST uses a bounded window specific to each ICU stay:
--
--   Rationale:
--   - AST is a ROUTINE lab, measured frequently in ICU as part of chemistry panels
--   - We want the ICU admission baseline (or immediately preceding value)
--   - Start: intime - 6 hours captures pre-ICU labs (ED, floor) that reflect admission state
--   - End: outtime ensures we only capture labs from THIS ICU stay, preventing contamination
--   - Without outtime bound, joining on subject_id could capture labs from future ICU stays
--   - This provides clean temporal boundaries for each ICU episode
--
--   Implementation:
--   - Join on subject_id (not hadm_id, as AST can be checked across admissions)
--   - Order by charttime (first chronologically within window)
--   - Bounded to [intime - 6h, outtime] to prevent inter-stay contamination
--   - Time offset exposed in output (minutes from ICU admission)
--
-- CLINICAL CONTEXT:
--   AST (Aspartate Aminotransferase) is an enzyme found in multiple tissues including
--   liver, heart, skeletal muscle, kidneys, and red blood cells. When these tissues
--   are damaged, AST is released into the bloodstream. While less liver-specific than
--   ALT, AST is routinely measured as part of liver function panels in ICU.
--
--   Part of standard liver function panel and ROUTINELY measured in ICU.
--
--   Typical use cases in ICU:
--   - Liver function assessment and monitoring (with ALT)
--   - Detection of hepatocellular injury (drug-induced, ischemic, viral)
--   - Myocardial infarction assessment (though troponins are preferred)
--   - Rhabdomyolysis and muscle injury detection
--   - Shock liver (ischemic hepatitis) identification
--   - Sepsis-associated liver dysfunction
--   - Part of admission workup for critically ill patients
--
--   AST vs ALT:
--   - AST is found in liver, heart, muscle, kidney (less specific)
--   - ALT is more liver-specific (primarily hepatocytes)
--   - AST/ALT ratio (De Ritis ratio) has diagnostic value:
--     * Ratio <1: Typical of acute viral hepatitis, NAFLD
--     * Ratio >2: Suggests alcoholic liver disease
--     * Ratio 1-2: Non-specific, seen in various conditions
--   - AST also elevated in cardiac events, muscle injury, hemolysis
--
--   Note: AST levels can be affected by:
--   - Hepatocellular injury (cirrhosis, hepatitis, drug toxicity, ischemia)
--   - Cardiac injury (MI, myocarditis, cardiac surgery)
--   - Muscle injury (rhabdomyolysis, trauma, strenuous exercise)
--   - Hemolysis (can cause spurious elevations)
--   - Certain medications (statins, antibiotics, anticonvulsants)
--   - Pancreatitis, renal infarction
--
--   Reference Ranges:
--   - Normal: 10-40 U/L (varies by lab, gender, and age)
--   - Mild elevation: 1-2x upper limit of normal (ULN)
--   - Moderate elevation: 2-10x ULN
--   - Marked elevation: >10x ULN (suggests acute tissue injury)
--   - Massive elevation: >1000 U/L (shock liver, acute hepatitis, massive MI)
--
--   Clinical Interpretation:
--   - <50 U/L: Generally considered normal to mildly elevated
--   - 50-150 U/L: Mild elevation (chronic liver disease, muscle injury)
--   - 150-500 U/L: Moderate elevation (hepatitis, MI, muscle injury)
--   - 500-3000 U/L: Significant elevation (shock liver, acute hepatitis)
--   - >3000 U/L: Severe injury (massive hepatic necrosis, severe shock liver)
--
--   Pattern Recognition in ICU:
--   - AST >> ALT (ratio >2): Consider cardiac injury, muscle injury, alcoholic liver disease
--   - AST ≈ ALT (ratio ~1): Acute viral hepatitis, drug-induced liver injury
--   - AST < ALT (ratio <1): NAFLD, chronic hepatitis
--   - Very high AST + high lactate: Consider shock liver (ischemic hepatitis)
-- ===============================================================================

DROP TABLE IF EXISTS icu_first_ast;

CREATE TABLE icu_first_ast AS
WITH ast_measurements AS (
    -- Extract first AST measurement within ICU stay time window
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
        50878       -- Aspartate Aminotransferase (AST) - IU/L or U/L
        -- =====================================================================
    )
    -- =========================================================================
    -- VALUE RANGE FILTERS - Edit these thresholds as needed
    -- =========================================================================
    AND le.valuenum > 0         -- Lower limit: lab values must be positive
    AND le.valuenum <= 10000    -- Upper limit: filters extreme outliers (U/L)
    -- Normal range: 10-40 U/L (varies by lab, gender, age)
    -- Mild elevation: <150 U/L
    -- Moderate elevation: 150-500 U/L
    -- Significant elevation: 500-3000 U/L
    -- Severe elevation: >3000 U/L (massive tissue injury)
    -- Values >10000 U/L are extremely rare (measurement errors or extreme injury)
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

    -- First AST measurement (within ICU stay bounded window)
    am.valuenum AS ast_first,
    am.charttime AS ast_first_charttime,
    am.itemid AS ast_first_itemid,

    -- Time from ICU admission to measurement (in minutes)
    -- Can be NEGATIVE (measured before ICU admission, within -6h window)
    -- or POSITIVE (measured during ICU stay)
    am.seconds_from_intime / 60.0 AS ast_first_minutes_from_intime

FROM icustays ie
LEFT JOIN ast_measurements am
    ON ie.icustay_id = am.icustay_id
    AND am.rn = 1  -- Only the first measurement (chronologically)
ORDER BY ie.icustay_id;


-- ===============================================================================
-- DIAGNOSTIC QUERIES
-- ===============================================================================
-- These queries help assess data completeness and quality.
-- Uncomment and run as needed for data validation.

-- Check data completeness: How many ICU stays have AST measurements?
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     SUM(CASE WHEN ast_first IS NOT NULL THEN 1 ELSE 0 END) AS stays_with_ast,
--     ROUND(100.0 * SUM(CASE WHEN ast_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_ast
-- FROM icu_first_ast;

-- Distribution of AST values
-- SELECT
--     MIN(ast_first) AS min_ast,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ast_first) AS p25_ast,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ast_first) AS median_ast,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ast_first) AS p75_ast,
--     MAX(ast_first) AS max_ast
-- FROM icu_first_ast
-- WHERE ast_first IS NOT NULL;

-- Check ITEMID distribution (should only be 50878 unless ITEMIDs are modified)
-- SELECT
--     ast_first_itemid,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_ast
-- WHERE ast_first_itemid IS NOT NULL
-- GROUP BY ast_first_itemid
-- ORDER BY n_stays DESC;

-- Timing analysis: When are first AST measurements typically taken relative to ICU admission?
-- SELECT
--     CASE
--         WHEN ast_first_minutes_from_intime < -300 THEN '5-6 hours before ICU'
--         WHEN ast_first_minutes_from_intime < -240 THEN '4-5 hours before ICU'
--         WHEN ast_first_minutes_from_intime < -180 THEN '3-4 hours before ICU'
--         WHEN ast_first_minutes_from_intime < -120 THEN '2-3 hours before ICU'
--         WHEN ast_first_minutes_from_intime < -60 THEN '1-2 hours before ICU'
--         WHEN ast_first_minutes_from_intime < 0 THEN 'Within 1h before ICU'
--         WHEN ast_first_minutes_from_intime <= 60 THEN 'Within 1h after ICU'
--         WHEN ast_first_minutes_from_intime <= 360 THEN '1-6 hours after ICU'
--         WHEN ast_first_minutes_from_intime <= 1440 THEN '6-24 hours after ICU'
--         ELSE 'More than 24h after ICU'
--     END AS timing_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_ast
-- WHERE ast_first IS NOT NULL
-- GROUP BY timing_category
-- ORDER BY MIN(ast_first_minutes_from_intime);

-- Check AST level categories (tissue injury assessment)
-- SELECT
--     CASE
--         WHEN ast_first <= 40 THEN 'Normal (≤ 40)'
--         WHEN ast_first <= 150 THEN 'Mildly Elevated (41-150)'
--         WHEN ast_first <= 500 THEN 'Moderately Elevated (151-500)'
--         WHEN ast_first <= 3000 THEN 'Significantly Elevated (501-3000)'
--         ELSE 'Severely Elevated (> 3000) - Massive Tissue Injury'
--     END AS ast_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_ast
-- WHERE ast_first IS NOT NULL
-- GROUP BY ast_category
-- ORDER BY MIN(ast_first);

-- Check AST by clinical significance thresholds
-- SELECT
--     CASE
--         WHEN ast_first < 150 THEN 'Normal/Mild (< 150) - Low clinical concern'
--         WHEN ast_first < 500 THEN 'Moderate (150-499) - Monitor, investigate'
--         WHEN ast_first < 3000 THEN 'Significant (500-2999) - Acute injury likely'
--         ELSE 'Severe (≥ 3000) - Massive injury, urgent workup'
--     END AS clinical_significance,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_ast
-- WHERE ast_first IS NOT NULL
-- GROUP BY clinical_significance
-- ORDER BY MIN(ast_first);

-- Analyze measurements captured before vs during ICU stay
-- SELECT
--     CASE
--         WHEN ast_first_minutes_from_intime < 0 THEN 'Before ICU admission (within -6h window)'
--         ELSE 'During ICU stay'
--     END AS measurement_timing,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
--     ROUND(AVG(ast_first), 2) AS avg_ast,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ast_first), 2) AS median_ast
-- FROM icu_first_ast
-- WHERE ast_first IS NOT NULL
-- GROUP BY measurement_timing
-- ORDER BY MIN(ast_first_minutes_from_intime);

-- Advanced: AST/ALT ratio analysis (requires icu_first_alt table)
-- This helps distinguish types of liver/tissue injury
-- SELECT
--     CASE
--         WHEN (ast.ast_first / alt.alt_first) < 1 THEN 'AST/ALT < 1 (Chronic hepatitis, NAFLD)'
--         WHEN (ast.ast_first / alt.alt_first) <= 2 THEN 'AST/ALT 1-2 (Non-specific)'
--         ELSE 'AST/ALT > 2 (Alcoholic liver disease, cardiac/muscle injury)'
--     END AS ast_alt_ratio_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
--     ROUND(AVG(ast.ast_first / alt.alt_first), 2) AS avg_ratio
-- FROM icu_first_ast ast
-- INNER JOIN icu_first_alt alt
--     ON ast.icustay_id = alt.icustay_id
-- WHERE ast.ast_first IS NOT NULL
--   AND alt.alt_first IS NOT NULL
--   AND alt.alt_first > 0  -- Prevent division by zero
-- GROUP BY ast_alt_ratio_category
-- ORDER BY MIN(ast.ast_first / alt.alt_first);
