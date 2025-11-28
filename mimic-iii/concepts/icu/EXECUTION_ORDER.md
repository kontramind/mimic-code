# ICU Scripts Execution Order for icu_stay_complete_* Tables

## Overview
This document specifies the correct execution order for all ICU concept scripts to ensure `icu_stay_complete_duckdb.sql` and `icu_stay_complete_with_orphans_duckdb.sql` work properly.

**Key Principle:** Execute scripts in dependency order - no script can run until all its dependencies exist.

---

## Dependency Levels

### Level 0: Base MIMIC-III Tables (Must Pre-exist)
These are standard MIMIC-III tables. No scripts need to be run; they should already exist in your database.

```
✓ icustays       - ICU stay records
✓ patients       - Patient demographics
✓ admissions     - Hospital admission records
✓ chartevents    - Charted vital signs and continuous measurements
✓ labevents      - Laboratory measurement results
```

---

### Level 1: Core Demographic & Administrative Tables (Execute First)
These tables can be created in **any order** - they only depend on Level 0 tables.

**Scripts to run (in any order):**

1. **icustay_detail_duckdb.sql**
   - Creates: `icustay_detail`
   - Dependencies: icustays, patients, admissions
   - Purpose: Core demographic/administrative data per ICU stay

2. **icu_age_duckdb.sql**
   - Creates: `icu_age`
   - Dependencies: icustays, patients
   - Purpose: Age at ICU admission (HIPAA-corrected)

3. **icu_readmission_30d_duckdb.sql**
   - Creates: `icu_readmission_30d`
   - Dependencies: icustays
   - Purpose: 30-day readmission flags (forward & backward-looking)

---

### Level 2: First Measurement Tables (Execute After Level 1)
These tables can be created in **any order** - they only depend on Level 0 and each other is independent.

**Vital Signs (3 scripts):**

4. **icu_first_hr_duckdb.sql**
   - Creates: `icu_first_hr`
   - Dependencies: icustays, chartevents
   - Purpose: First heart rate measurement

5. **icu_first_bp_duckdb.sql**
   - Creates: `icu_first_bp`
   - Dependencies: icustays, chartevents
   - Purpose: First systolic & diastolic blood pressure

6. **icu_first_resprate_duckdb.sql**
   - Creates: `icu_first_resprate`
   - Dependencies: icustays, chartevents
   - Purpose: First respiratory rate measurement

**Laboratory Values (5 scripts):**

7. **icu_first_ntprobnp_duckdb.sql**
   - Creates: `icu_first_ntprobnp`
   - Dependencies: icustays, labevents
   - Purpose: First NT-proBNP (cardiac biomarker)

8. **icu_first_creatinine_duckdb.sql**
   - Creates: `icu_first_creatinine`
   - Dependencies: icustays, labevents
   - Purpose: First creatinine (renal function)

9. **icu_first_bun_duckdb.sql**
   - Creates: `icu_first_bun`
   - Dependencies: icustays, labevents
   - Purpose: First BUN (renal function)

10. **icu_first_potassium_duckdb.sql**
    - Creates: `icu_first_potassium`
    - Dependencies: icustays, labevents
    - Purpose: First potassium (electrolyte)

11. **icu_first_total_cholesterol_duckdb.sql**
    - Creates: `icu_first_total_cholesterol`
    - Dependencies: icustays, labevents
    - Purpose: First total cholesterol (lipid panel)

**Anthropometrics (2 scripts):**

12. **icu_first_height_duckdb.sql**
    - Creates: `icu_first_height`
    - Dependencies: icustays, chartevents
    - Purpose: First height measurement (with unit conversion)

13. **icu_first_weight_duckdb.sql**
    - Creates: `icu_first_weight`
    - Dependencies: icustays, chartevents
    - Purpose: First weight measurement (with hierarchy logic)

---

### Level 3: Composite/Integration Tables (Execute Last)
These tables must wait until ALL Level 1 and Level 2 tables exist.

14. **icu_stay_complete_duckdb.sql**
    - Creates: `icu_stay_complete`
    - Dependencies: ALL of the above (icustay_detail, icu_age, icu_readmission_30d, all icu_first_* tables, admissions)
    - Purpose: Comprehensive ICU stay dataset (filtered to stays with chart events)
    - Note: Uses `icustay_detail` as base table

15. **icu_stay_complete_with_orphans_duckdb.sql**
    - Creates: `icu_stay_complete_with_orphans`
    - Dependencies: ALL of the above (same as icu_stay_complete)
    - Purpose: Complete ICU stay dataset (includes orphan stays without admissions records)
    - Note: Uses `icustays` directly as base table (includes all ~61K stays)

---

## Recommended Execution Strategy

### Option A: Sequential Execution (Safest, Slower)
```bash
# Level 1: Core tables
duckdb < icustay_detail_duckdb.sql
duckdb < icu_age_duckdb.sql
duckdb < icu_readmission_30d_duckdb.sql

# Level 2: First measurement tables
duckdb < icu_first_hr_duckdb.sql
duckdb < icu_first_bp_duckdb.sql
duckdb < icu_first_resprate_duckdb.sql
duckdb < icu_first_ntprobnp_duckdb.sql
duckdb < icu_first_creatinine_duckdb.sql
duckdb < icu_first_bun_duckdb.sql
duckdb < icu_first_potassium_duckdb.sql
duckdb < icu_first_total_cholesterol_duckdb.sql
duckdb < icu_first_height_duckdb.sql
duckdb < icu_first_weight_duckdb.sql

# Level 3: Composite tables
duckdb < icu_stay_complete_duckdb.sql
duckdb < icu_stay_complete_with_orphans_duckdb.sql
```

### Option B: Parallel Execution (Faster)
Within each level, scripts can execute in parallel since they don't depend on each other.

```bash
# Level 1: Run all 3 in parallel
(duckdb < icustay_detail_duckdb.sql) &
(duckdb < icu_age_duckdb.sql) &
(duckdb < icu_readmission_30d_duckdb.sql) &
wait  # Wait for all Level 1 to complete

# Level 2: Run all 10 in parallel
(duckdb < icu_first_hr_duckdb.sql) &
(duckdb < icu_first_bp_duckdb.sql) &
(duckdb < icu_first_resprate_duckdb.sql) &
(duckdb < icu_first_ntprobnp_duckdb.sql) &
(duckdb < icu_first_creatinine_duckdb.sql) &
(duckdb < icu_first_bun_duckdb.sql) &
(duckdb < icu_first_potassium_duckdb.sql) &
(duckdb < icu_first_total_cholesterol_duckdb.sql) &
(duckdb < icu_first_height_duckdb.sql) &
(duckdb < icu_first_weight_duckdb.sql) &
wait  # Wait for all Level 2 to complete

# Level 3: Run final tables (can also parallel)
(duckdb < icu_stay_complete_duckdb.sql) &
(duckdb < icu_stay_complete_with_orphans_duckdb.sql) &
wait
```

---

## Critical Dependencies by Script

| Script | Requires These to Exist | Must Exist Before |
|--------|-------------------------|-------------------|
| icustay_detail | icustays, patients, admissions | icu_stay_complete |
| icu_age | icustays, patients | icu_stay_complete |
| icu_readmission_30d | icustays | icu_stay_complete |
| icu_first_hr | icustays, chartevents | icu_stay_complete |
| icu_first_bp | icustays, chartevents | icu_stay_complete |
| icu_first_resprate | icustays, chartevents | icu_stay_complete |
| icu_first_ntprobnp | icustays, labevents | icu_stay_complete |
| icu_first_creatinine | icustays, labevents | icu_stay_complete |
| icu_first_bun | icustays, labevents | icu_stay_complete |
| icu_first_potassium | icustays, labevents | icu_stay_complete |
| icu_first_total_cholesterol | icustays, labevents | icu_stay_complete |
| icu_first_height | icustays, chartevents | icu_stay_complete |
| icu_first_weight | icustays, chartevents | icu_stay_complete |
| **icu_stay_complete** | ALL 13 tables above | Final use |
| **icu_stay_complete_with_orphans** | ALL 13 tables above | Final use |

---

## Practical Notes

### Data Completeness
- **icu_stay_complete**: ~44K ICU stays (filtered to stays with chart events and admissions match)
- **icu_stay_complete_with_orphans**: ~61.5K ICU stays (all records, including ~17K orphan stays)

### Time Windows
- **Vitals** (HR, BP, RR): ±6 hours from ICU admission
- **Routine Labs** (creatinine, BUN, potassium): ±6 hours from ICU admission
- **Sparse Labs** (NT-proBNP, cholesterol): ±7 days from ICU admission
- **Anthropometrics** (height, weight): Full ICU stay window

### NULL Handling
- NULL values indicate measurement not available during the time window
- This is intentional - scripts do not impute or substitute missing values
- Use filtering in analysis if complete data required (see examples in icu_stay_complete script)

### Dependencies Not Required
The following scripts exist but are **NOT prerequisites** for icu_stay_complete:
- icu_first_temperature_duckdb.sql (optional, not joined in composite table)
- icu_first_spo2_duckdb.sql (optional, not joined in composite table)
- icu_primary_diagnosis_duckdb.sql (ICD-9 codes, not item IDs)
- Other future measurements

---

## Testing/Verification

After all scripts complete, verify successful creation:

```sql
-- Check all required tables exist
SELECT
    'icustay_detail' as table_name, COUNT(*) as row_count FROM icustay_detail
UNION ALL SELECT 'icu_age', COUNT(*) FROM icu_age
UNION ALL SELECT 'icu_readmission_30d', COUNT(*) FROM icu_readmission_30d
UNION ALL SELECT 'icu_first_hr', COUNT(*) FROM icu_first_hr
UNION ALL SELECT 'icu_first_bp', COUNT(*) FROM icu_first_bp
UNION ALL SELECT 'icu_first_resprate', COUNT(*) FROM icu_first_resprate
UNION ALL SELECT 'icu_first_ntprobnp', COUNT(*) FROM icu_first_ntprobnp
UNION ALL SELECT 'icu_first_creatinine', COUNT(*) FROM icu_first_creatinine
UNION ALL SELECT 'icu_first_bun', COUNT(*) FROM icu_first_bun
UNION ALL SELECT 'icu_first_potassium', COUNT(*) FROM icu_first_potassium
UNION ALL SELECT 'icu_first_total_cholesterol', COUNT(*) FROM icu_first_total_cholesterol
UNION ALL SELECT 'icu_first_height', COUNT(*) FROM icu_first_height
UNION ALL SELECT 'icu_first_weight', COUNT(*) FROM icu_first_weight
UNION ALL SELECT 'icu_stay_complete', COUNT(*) FROM icu_stay_complete
UNION ALL SELECT 'icu_stay_complete_with_orphans', COUNT(*) FROM icu_stay_complete_with_orphans
ORDER BY table_name;
```

Expected output:
- icustay_detail: ~44,574 rows
- icu_age: ~61,532 rows
- icu_readmission_30d: ~61,532 rows
- Each icu_first_*: ~61,532 rows (with varying % NULL)
- icu_stay_complete: ~44,574 rows
- icu_stay_complete_with_orphans: ~61,532 rows

---

## Summary Table

**Execution Sequence:**

```
LEVEL 1 (Any order):
├── icustay_detail
├── icu_age
└── icu_readmission_30d

        ↓ (Wait for Level 1)

LEVEL 2 (Any order):
├── icu_first_hr
├── icu_first_bp
├── icu_first_resprate
├── icu_first_ntprobnp
├── icu_first_creatinine
├── icu_first_bun
├── icu_first_potassium
├── icu_first_total_cholesterol
├── icu_first_height
└── icu_first_weight

        ↓ (Wait for Level 2)

LEVEL 3 (Either order):
├── icu_stay_complete
└── icu_stay_complete_with_orphans
```

---

## Summary

- **Total scripts:** 15 DuckDB concept scripts
- **Prerequisites:** 5 base MIMIC-III tables
- **Sequential execution time:** ~15-30 minutes (depending on database size)
- **Parallel execution time:** ~10-15 minutes (Level 1 ≈1-2 min, Level 2 ≈5-10 min, Level 3 ≈1-2 min)
- **Total rows in icu_stay_complete:** ~44,574 ICU stays
- **Total rows with orphans:** ~61,532 ICU stays
