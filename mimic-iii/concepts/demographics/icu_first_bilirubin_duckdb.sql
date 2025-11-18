-- ===============================================================================
-- MIMIC-III DuckDB: First Total Bilirubin per ICU Stay
-- ===============================================================================
-- This query creates a table with the first Total Bilirubin measurement
-- for each ICU stay in the MIMIC-III database, optimized for DuckDB.
--
-- The table includes:
--   - First Total Bilirubin value within time window (mg/dL)
--   - Timestamp of the measurement
--   - ITEMID used (for tracking measurement source)
--   - Minutes from ICU admission to measurement
--
-- ITEMID Configuration:
--   To modify which ITEMIDs are considered for Total Bilirubin, edit the list
--   in the WHERE clause within the bili_measurements CTE below.
--
-- Design Notes:
--   - Uses TABLE (not VIEW) for better performance and reusability
--   - No filters by default - includes all ICU stays for maximum flexibility
--   - Tracks ITEMID to identify measurement source
--   - NULL values indicate no Total Bilirubin measurement during the defined time window
--   - Uses labevents table (laboratory values, not vital signs)
--   - Part of routine liver function panel (see also: ALT, AST, ALP, Direct/Indirect Bilirubin)
--
-- Unit of Analysis: ICU stays (icustay_id)
--   - Each ICU stay is analyzed independently
--   - One patient can have multiple hospital admissions
--   - One hospital admission can have multiple ICU stays
--   - We find the "first" measurement for EACH ICU stay
--
-- DECISION: Time Window from "intime - 6 hours" to "outtime" (ICU Stay Bounded)
--   Unlike sparse labs (lipid panels) which use "closest within hospital admission"
--   approach, Total Bilirubin uses a bounded window specific to each ICU stay:
--
--   Rationale:
--   - Total Bilirubin is a ROUTINE lab, measured frequently in ICU as part of chemistry panels
--   - We want the ICU admission baseline (or immediately preceding value)
--   - Start: intime - 6 hours captures pre-ICU labs (ED, floor) that reflect admission state
--   - End: outtime ensures we only capture labs from THIS ICU stay, preventing contamination
--   - Without outtime bound, joining on subject_id could capture labs from future ICU stays
--   - This provides clean temporal boundaries for each ICU episode
--
--   Implementation:
--   - Join on subject_id (not hadm_id, as bilirubin can be checked across admissions)
--   - Order by charttime (first chronologically within window)
--   - Bounded to [intime - 6h, outtime] to prevent inter-stay contamination
--   - Time offset exposed in output (minutes from ICU admission)
--
-- CLINICAL CONTEXT:
--   Total Bilirubin measures both conjugated (direct) and unconjugated (indirect)
--   bilirubin in the blood. Bilirubin is a breakdown product of hemoglobin from
--   red blood cells. Elevated levels indicate problems with liver function, bile duct
--   obstruction, or increased red blood cell destruction (hemolysis).
--
--   Part of standard liver function panel and ROUTINELY measured in ICU.
--
--   Typical use cases in ICU:
--   - Liver function assessment and monitoring
--   - Detection of hepatic dysfunction (cirrhosis, acute liver failure)
--   - Bile duct obstruction identification (cholestasis)
--   - Hemolysis detection (increased unconjugated bilirubin)
--   - Sepsis-associated liver dysfunction
--   - Post-operative monitoring (especially cardiac/liver surgery)
--   - Part of SOFA score calculation (Sequential Organ Failure Assessment)
--   - Monitoring hepatotoxic medications
--
--   Total Bilirubin = Direct (conjugated) + Indirect (unconjugated)
--   - Direct bilirubin: Water-soluble, processed by liver, excreted in bile
--   - Indirect bilirubin: Fat-soluble, not yet processed by liver
--
--   Pattern Recognition:
--   - Elevated Total + Elevated Direct (>50% of total): Liver disease or obstruction
--   - Elevated Total + Normal/Low Direct (<20% of total): Hemolysis or Gilbert's syndrome
--   - Very high Total (>12 mg/dL): Severe liver dysfunction or complete bile duct obstruction
--
--   Note: Bilirubin levels can be affected by:
--   - Liver disease (hepatitis, cirrhosis, liver failure)
--   - Bile duct obstruction (gallstones, tumors, strictures)
--   - Hemolysis (destruction of red blood cells)
--   - Gilbert's syndrome (common benign condition)
--   - Sepsis and critical illness
--   - Certain medications (can cause cholestasis)
--   - Total parenteral nutrition (TPN)
--
--   Reference Ranges:
--   - Normal Total Bilirubin: 0.1-1.2 mg/dL
--   - Mild elevation: 1.2-3.0 mg/dL (often subclinical)
--   - Moderate elevation: 3.0-6.0 mg/dL (clinical jaundice visible)
--   - Severe elevation: 6.0-12.0 mg/dL (significant hepatic dysfunction)
--   - Very severe elevation: >12.0 mg/dL (liver failure or complete obstruction)
--
--   Clinical Interpretation:
--   - <1.2 mg/dL: Normal
--   - 1.2-2.0 mg/dL: Mild elevation (Gilbert's, mild hemolysis, early liver disease)
--   - 2.0-3.0 mg/dL: Jaundice becomes clinically visible (scleral icterus)
--   - 3.0-10.0 mg/dL: Moderate hepatic dysfunction or obstruction
--   - >10.0 mg/dL: Severe liver disease, complete obstruction, or severe hemolysis
--
--   SOFA Score (Severity Assessment):
--   - Bilirubin <1.2 mg/dL: 0 points (normal)
--   - Bilirubin 1.2-1.9 mg/dL: 1 point
--   - Bilirubin 2.0-5.9 mg/dL: 2 points
--   - Bilirubin 6.0-11.9 mg/dL: 3 points
--   - Bilirubin ≥12.0 mg/dL: 4 points (severe liver dysfunction)
--
--   Jaundice (Icterus) Visibility:
--   - Not visible: <2.0 mg/dL
--   - Scleral icterus (eyes): 2.0-3.0 mg/dL
--   - Skin jaundice: >3.0 mg/dL
-- ===============================================================================

DROP TABLE IF EXISTS icu_first_bilirubin;

CREATE TABLE icu_first_bilirubin AS
WITH bili_measurements AS (
    -- Extract first Total Bilirubin measurement within ICU stay time window
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
        50885       -- Total Bilirubin - mg/dL
        -- Note: Related ITEMIDs (not included here):
        -- 50883 = Direct Bilirubin (conjugated)
        -- 50884 = Indirect Bilirubin (unconjugated)
        -- =====================================================================
    )
    -- =========================================================================
    -- VALUE RANGE FILTERS - Edit these thresholds as needed
    -- =========================================================================
    AND le.valuenum >= 0        -- Lower limit: bilirubin cannot be negative
    AND le.valuenum <= 150      -- Upper limit: filters extreme outliers (mg/dL)
    -- Normal range: 0.1-1.2 mg/dL
    -- Clinical jaundice: >2.0 mg/dL
    -- Severe elevation: >10 mg/dL
    -- Values >150 mg/dL are extremely rare (severe liver failure/measurement errors)
    -- Note: Newborns can have much higher values, but MIMIC-III is adult ICU
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

    -- First Total Bilirubin measurement (within ICU stay bounded window)
    bm.valuenum AS bilirubin_first,
    bm.charttime AS bilirubin_first_charttime,
    bm.itemid AS bilirubin_first_itemid,

    -- Time from ICU admission to measurement (in minutes)
    -- Can be NEGATIVE (measured before ICU admission, within -6h window)
    -- or POSITIVE (measured during ICU stay)
    bm.seconds_from_intime / 60.0 AS bilirubin_first_minutes_from_intime

FROM icustays ie
LEFT JOIN bili_measurements bm
    ON ie.icustay_id = bm.icustay_id
    AND bm.rn = 1  -- Only the first measurement (chronologically)
ORDER BY ie.icustay_id;


-- ===============================================================================
-- DIAGNOSTIC QUERIES
-- ===============================================================================
-- These queries help assess data completeness and quality.
-- Uncomment and run as needed for data validation.

-- Check data completeness: How many ICU stays have Total Bilirubin measurements?
-- SELECT
--     COUNT(*) AS total_icu_stays,
--     SUM(CASE WHEN bilirubin_first IS NOT NULL THEN 1 ELSE 0 END) AS stays_with_bilirubin,
--     ROUND(100.0 * SUM(CASE WHEN bilirubin_first IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_bilirubin
-- FROM icu_first_bilirubin;

-- Distribution of Total Bilirubin values
-- SELECT
--     MIN(bilirubin_first) AS min_bili,
--     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY bilirubin_first) AS p25_bili,
--     PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY bilirubin_first) AS median_bili,
--     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY bilirubin_first) AS p75_bili,
--     MAX(bilirubin_first) AS max_bili
-- FROM icu_first_bilirubin
-- WHERE bilirubin_first IS NOT NULL;

-- Check ITEMID distribution (should only be 50885 unless ITEMIDs are modified)
-- SELECT
--     bilirubin_first_itemid,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_bilirubin
-- WHERE bilirubin_first_itemid IS NOT NULL
-- GROUP BY bilirubin_first_itemid
-- ORDER BY n_stays DESC;

-- Timing analysis: When are first Total Bilirubin measurements typically taken relative to ICU admission?
-- SELECT
--     CASE
--         WHEN bilirubin_first_minutes_from_intime < -300 THEN '5-6 hours before ICU'
--         WHEN bilirubin_first_minutes_from_intime < -240 THEN '4-5 hours before ICU'
--         WHEN bilirubin_first_minutes_from_intime < -180 THEN '3-4 hours before ICU'
--         WHEN bilirubin_first_minutes_from_intime < -120 THEN '2-3 hours before ICU'
--         WHEN bilirubin_first_minutes_from_intime < -60 THEN '1-2 hours before ICU'
--         WHEN bilirubin_first_minutes_from_intime < 0 THEN 'Within 1h before ICU'
--         WHEN bilirubin_first_minutes_from_intime <= 60 THEN 'Within 1h after ICU'
--         WHEN bilirubin_first_minutes_from_intime <= 360 THEN '1-6 hours after ICU'
--         WHEN bilirubin_first_minutes_from_intime <= 1440 THEN '6-24 hours after ICU'
--         ELSE 'More than 24h after ICU'
--     END AS timing_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_bilirubin
-- WHERE bilirubin_first IS NOT NULL
-- GROUP BY timing_category
-- ORDER BY MIN(bilirubin_first_minutes_from_intime);

-- Check Total Bilirubin level categories (liver function assessment)
-- SELECT
--     CASE
--         WHEN bilirubin_first <= 1.2 THEN 'Normal (≤ 1.2)'
--         WHEN bilirubin_first <= 2.0 THEN 'Mildly Elevated (1.2-2.0)'
--         WHEN bilirubin_first <= 3.0 THEN 'Jaundice Threshold (2.0-3.0)'
--         WHEN bilirubin_first <= 6.0 THEN 'Moderate Elevation (3.0-6.0)'
--         WHEN bilirubin_first <= 12.0 THEN 'Severe Elevation (6.0-12.0)'
--         ELSE 'Very Severe (> 12.0) - Critical Liver Dysfunction'
--     END AS bilirubin_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_bilirubin
-- WHERE bilirubin_first IS NOT NULL
-- GROUP BY bilirubin_category
-- ORDER BY MIN(bilirubin_first);

-- Check Total Bilirubin by SOFA score categories (severity assessment)
-- SELECT
--     CASE
--         WHEN bilirubin_first < 1.2 THEN 'SOFA 0: Normal (< 1.2)'
--         WHEN bilirubin_first < 2.0 THEN 'SOFA 1: Mild (1.2-1.9)'
--         WHEN bilirubin_first < 6.0 THEN 'SOFA 2: Moderate (2.0-5.9)'
--         WHEN bilirubin_first < 12.0 THEN 'SOFA 3: Severe (6.0-11.9)'
--         ELSE 'SOFA 4: Critical (≥ 12.0)'
--     END AS sofa_category,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_bilirubin
-- WHERE bilirubin_first IS NOT NULL
-- GROUP BY sofa_category
-- ORDER BY MIN(bilirubin_first);

-- Jaundice visibility thresholds
-- SELECT
--     CASE
--         WHEN bilirubin_first < 2.0 THEN 'Not Visible (< 2.0)'
--         WHEN bilirubin_first < 3.0 THEN 'Scleral Icterus (2.0-3.0)'
--         ELSE 'Clinically Visible Jaundice (> 3.0)'
--     END AS jaundice_visibility,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
-- FROM icu_first_bilirubin
-- WHERE bilirubin_first IS NOT NULL
-- GROUP BY jaundice_visibility
-- ORDER BY MIN(bilirubin_first);

-- Analyze measurements captured before vs during ICU stay
-- SELECT
--     CASE
--         WHEN bilirubin_first_minutes_from_intime < 0 THEN 'Before ICU admission (within -6h window)'
--         ELSE 'During ICU stay'
--     END AS measurement_timing,
--     COUNT(*) AS n_stays,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
--     ROUND(AVG(bilirubin_first), 2) AS avg_bilirubin,
--     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY bilirubin_first), 2) AS median_bilirubin
-- FROM icu_first_bilirubin
-- WHERE bilirubin_first IS NOT NULL
-- GROUP BY measurement_timing
-- ORDER BY MIN(bilirubin_first_minutes_from_intime);
