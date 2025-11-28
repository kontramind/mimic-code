# ICU Concepts - MIMIC-III DuckDB

This directory contains DuckDB SQL scripts for extracting and analyzing ICU-related data from MIMIC-III. All scripts follow a unified, pragmatic approach to time boundaries and data extraction.

## Time Boundary Convention

**All ICU-related queries use ADMINISTRATIVE ICU TIMES** from the `icustays` table:
- **`intime`**: Official ICU admission timestamp
- **`outtime`**: Official ICU discharge timestamp

This is the standard, pragmatic approach used across MIMIC analyses.

### Why Administrative Times (Not Clinical HR-Based Boundaries)?

1. **Transparency**: Direct use of official ICU records - no intermediate calculations
2. **Simplicity**: No fuzzy boundaries, no intermediate tables, no complex logic
3. **Standard Practice**: Aligns with MIMIC-III "firstday" concept patterns and pragmatic workflows
4. **Reproducibility**: Easy to understand, explain, and replicate
5. **No Dependencies**: Each script depends only on base MIMIC tables (icustays, chartevents, labevents, etc.)

## Script Categories

### Foundational Tables
- **`icustay_detail_duckdb.sql`** - Comprehensive demographic and administrative ICU stay information
- **`icu_age_duckdb.sql`** - Patient age at ICU admission (with HIPAA de-identification handling)
- **`icu_readmission_30d_duckdb.sql`** - 30-day ICU readmission indicators
- **`icu_primary_diagnosis_duckdb.sql`** - Primary diagnosis for ICU stays
- **`icu_stay_complete_duckdb.sql`** - Complete ICU stay data combining demographics and clinical variables

### First Measurements (Baseline Values)

All "first" measurements are extracted as the **earliest value within the ICU stay time window** [intime, outtime], with some measurement types allowing a lookback window:

#### Vital Signs
- `icu_first_vitals_duckdb.sql` - Aggregated first vital signs
- `icu_first_hr_duckdb.sql` - First heart rate
- `icu_first_bp_duckdb.sql` - First blood pressure (systolic/diastolic)
- `icu_first_resprate_duckdb.sql` - First respiratory rate
- `icu_first_temperature_duckdb.sql` - First temperature
- `icu_first_spo2_duckdb.sql` - First oxygen saturation
- `icu_first_rhythm_duckdb.sql` - First cardiac rhythm

#### Laboratory Values
- `icu_first_creatinine_duckdb.sql` - First serum creatinine (renal function)
- `icu_first_bun_duckdb.sql` - First blood urea nitrogen (renal function)
- `icu_first_potassium_duckdb.sql` - First serum potassium (electrolyte)
- `icu_first_hemoglobin_duckdb.sql` - First hemoglobin (anemia assessment)
- `icu_first_hematocrit_duckdb.sql` - First hematocrit
- `icu_first_wbc_duckdb.sql` - First white blood cell count (infection/immunity)
- `icu_first_platelet_duckdb.sql` - First platelet count (coagulation)
- `icu_first_bilirubin_duckdb.sql` - First bilirubin (liver function)
- `icu_first_albumin_duckdb.sql` - First albumin (nutritional/liver status)
- `icu_first_alt_duckdb.sql` - First alanine aminotransferase (liver enzyme)
- `icu_first_ast_duckdb.sql` - First aspartate aminotransferase (liver enzyme)

#### Lipid Panel & Cardiac Markers
- `icu_first_total_cholesterol_duckdb.sql` - First total cholesterol
- `icu_first_ldl_duckdb.sql` - First LDL cholesterol
- `icu_first_hdl_duckdb.sql` - First HDL cholesterol
- `icu_first_triglycerides_duckdb.sql` - First triglycerides
- `icu_first_ntprobnp_duckdb.sql` - First NT-proBNP (cardiac biomarker)

#### Anthropometric Data
- `icu_first_height_duckdb.sql` - First height measurement
- `icu_first_weight_duckdb.sql` - First weight measurement

## Time Window Specifics

### Vital Signs & Common Labs
- **Window**: `intime - 6 hours` to `outtime`
- **Rationale**: Captures pre-ICU measurements from ED/floor that reflect admission baseline, while preventing contamination from future ICU stays
- **Affected tables**: HR, BP, temperature, SpO2, respiratory rate, creatinine, potassium, etc.

### Sparse Laboratory Values
- **Window**: Varies by measurement (typically broader, e.g., `-7 days` to `outtime`)
- **Rationale**: Labs like NT-proBNP, cholesterol are measured infrequently; broader windows ensure capture
- **Check individual scripts** for specific time windows

### Anthro pometrics (Height, Weight)
- **Window**: Full ICU stay (`intime` to `outtime`)
- **Rationale**: Initial measurements at admission most relevant

## Output Structure

Each script produces a table with:
- **Identifiers**: `icustay_id`, `subject_id`, `hadm_id`
- **ICU Timing**: `icu_intime`, `icu_outtime`
- **Measurement Value**: The extracted value (e.g., `creatinine_first`)
- **Timestamp**: When the measurement was taken (e.g., `creatinine_first_charttime`)
- **Source**: Item ID for reference (e.g., `creatinine_first_itemid`)
- **Time Delta**: Minutes from ICU admission to measurement (e.g., `creatinine_first_minutes_from_intime`)

## Usage Pattern

### Basic Query
```sql
SELECT * FROM icu_first_creatinine;
```

### Join With Other Tables
```sql
SELECT
    ic.icustay_id,
    ic.subject_id,
    ia.age,
    ia.gender,
    ifc.creatinine_first,
    ifhr.hr_first
FROM icu_age ia
INNER JOIN icustay_detail ic ON ia.icustay_id = ic.icustay_id
LEFT JOIN icu_first_creatinine ifc ON ia.icustay_id = ifc.icustay_id
LEFT JOIN icu_first_hr ifhr ON ia.icustay_id = ifhr.icustay_id
WHERE ia.age >= 18;
```

### Combine Into Cohort Table
```sql
CREATE TABLE icu_cohort_adult AS
SELECT
    ic.icustay_id,
    ic.subject_id,
    ic.hadm_id,
    ia.age,
    ia.gender,
    ifhr.hr_first,
    ifbp.sysbp_first,
    ifbp.diasbp_first,
    ifc.creatinine_first,
    ifbun.bun_first,
    ifpot.potassium_first,
    ififc.weight_first
FROM icustay_detail ic
LEFT JOIN icu_age ia ON ic.icustay_id = ia.icustay_id
LEFT JOIN icu_first_hr ifhr ON ic.icustay_id = ifhr.icustay_id
LEFT JOIN icu_first_bp ifbp ON ic.icustay_id = ifbp.icustay_id
LEFT JOIN icu_first_creatinine ifc ON ic.icustay_id = ifc.icustay_id
LEFT JOIN icu_first_bun ifbun ON ic.icustay_id = ifbun.icustay_id
LEFT JOIN icu_first_potassium ifpot ON ic.icustay_id = ifpot.icustay_id
LEFT JOIN icu_first_weight ififc ON ic.icustay_id = ififc.icustay_id
WHERE ia.age >= 18;
```

## NULL Handling

All scripts use `LEFT JOIN` to preserve ICU stays without measurements. A NULL value indicates:
- The measurement type was not recorded during the ICU stay
- Or the value fell outside acceptable physiological ranges

When building cohorts, decide whether to:
1. **Exclude NULLs**: `WHERE measurement IS NOT NULL` (for analyses requiring complete data)
2. **Include NULLs**: Keep them and note missingness in analysis

## Data Quality Notes

- **Age >89 years**: Shifted ~300 years into past for HIPAA de-identification in MIMIC-III
  - Raw ages stored as `age_raw` in `icu_age_duckdb.sql`
  - Corrected ages (>89 → 91.4) in `age` column
- **Item IDs**: Each script tracks source item ID (CareVue vs MetaVision) - check scripts for variations
- **Value Ranges**: All scripts filter for physiologically plausible ranges - see comments in scripts for thresholds
- **Outliers**: Data entry errors are filtered based on clinical knowledge

## Documentation For Publications

### Methods Section Example

> **Data Extraction**: Patient demographics, vital signs, and laboratory values were extracted from MIMIC-III using DuckDB. All measurements were temporally bounded to each patient's ICU admission and discharge times (intime/outtime). For routine vital signs and common labs, we extracted the earliest value within a window from 6 hours before ICU admission to discharge, capturing admission baseline measurements while preventing contamination from subsequent ICU stays. First measurements were identified chronologically, and patients with missing measurements were retained in analyses with missingness noted.

### Supplementary Methods Example

> **Time Boundaries**: All ICU-related analyses use administrative ICU times (intime/outtime from the icustays table). This pragmatic approach aligns with standard MIMIC concept extraction patterns. Measurements recorded within [intime - 6h, outtime] were considered part of each ICU episode. This ensures temporal specificity while allowing capture of pre-ICU measurements that reflect admission status. Item IDs are tracked to identify measurement source (CareVue vs MetaVision monitoring systems).

## Notes for Researchers

1. **No Intermediate Time Tables**: Unlike some MIMIC implementations, these scripts do not use clinical HR-based time windows (intime_hr/outtime_hr). They use direct administrative times for simplicity and transparency.

2. **DuckDB Specific**: All scripts are optimized for DuckDB syntax. PostgreSQL and BigQuery versions exist in parallel directories but may have dialect differences.

3. **Customization**: All scripts include comments marking places where you can edit:
   - Item ID lists (to include/exclude specific measurement sources)
   - Value range thresholds (to adjust outlier filtering)
   - Time windows (to adjust lookback/lookforward periods)

4. **Performance**: Tables are created with `CREATE TABLE` (not `CREATE VIEW`) for better performance when repeatedly joined.

## Script Dependencies

All scripts are **independent** - each depends only on base MIMIC tables:
- `icustays` - ICU stay metadata
- `patients` - Patient demographics
- `admissions` - Hospital admission information
- `chartevents` - Vital sign measurements
- `labevents` - Laboratory test results

No script depends on another script in this directory.

## Related Directories

- `../demographics/` - General demographic concepts (age, icustay_hours, etc.)
- `../firstday/` - Original MIMIC-III first day concepts (BigQuery/PostgreSQL)
- `../durations/` - Duration-based concepts
- `../organfailure/` - Organ failure scoring

---

**Last Updated**: 2025-11-28
**DuckDB Version**: Optimized for current DuckDB
**MIMIC-III Version**: v1.4
