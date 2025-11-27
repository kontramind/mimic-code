# Orphan ICU Stays Analysis

## What are Orphan ICU Stays?

"Orphan" ICU stays are ICU stay records in the `icustays` table that do not have matching records in the `admissions` table. These are ICU stays where either:
1. The `hadm_id` (hospital admission ID) is NULL, or
2. The `hadm_id` exists but there is no corresponding record in the `admissions` table

## Scale of the Issue

According to the MIMIC-III v1.4 database:
- **Total ICU stays**: ~61,532
- **Orphan ICU stays**: ~17,000 (approximately 27-28% of all ICU stays)
- **ICU stays with admission records**: ~44,000

## Why This Matters

Orphan ICU stays are significant because:

1. **Missing Hospital Context**: Without admission records, we lose important information such as:
   - Admission type (EMERGENCY, ELECTIVE, URGENT, NEWBORN)
   - Admission location
   - Discharge location
   - Hospital length of stay
   - Primary diagnosis information

2. **Data Completeness**: Many derived tables and analyses in MIMIC-III use `icustay_detail` as the base, which:
   - Filters to `has_chartevents_data = 1`
   - Requires matching admission records
   - Results in only ~44K ICU stays (excluding most orphans)

3. **Analysis Bias**: Excluding orphan stays could introduce selection bias in research studies

## SQL Queries

### Files for Analysis

1. **`count_orphan_icu_stays.sql`**: Queries to count and analyze orphan ICU stays
2. **`icu_stay_complete_with_orphans_duckdb.sql`**: Complete ICU stay dataset INCLUDING orphans
3. **`icu_stay_complete_duckdb.sql`**: Standard dataset (excludes most orphans)

### Key Differences Between Datasets

| Feature | `icu_stay_complete` | `icu_stay_complete_with_orphans` |
|---------|---------------------|----------------------------------|
| Base table | `icustay_detail` | `icustays` |
| Join type | INNER/LEFT with filtering | LEFT JOIN only |
| Record count | ~44,000 | ~61,532 |
| Includes orphans | No (mostly excluded) | Yes (all stays) |
| Has chartevents | Yes (required) | Not required |
| Use case | Standard clinical analysis | Complete coverage analysis |

## How to Use

### To Count Orphan Stays

```sql
-- Run the queries in count_orphan_icu_stays.sql
\i /home/user/mimic-code/mimic-iii/concepts/icu/count_orphan_icu_stays.sql
```

### To Include Orphans in Analysis

```sql
-- Use icu_stay_complete_with_orphans instead of icu_stay_complete
SELECT * FROM icu_stay_complete_with_orphans
WHERE has_admission_record = 0;  -- Filter to orphans only
```

### To Identify Data Quality Issues

The `icu_stay_complete_with_orphans` table includes two flags:
- `has_admission_record`: 1 if admission record exists, 0 if orphan
- `has_icustay_detail`: 1 if in icustay_detail table, 0 otherwise

## Recommendations

1. **For complete coverage**: Use `icu_stay_complete_with_orphans`
2. **For standard analysis**: Use `icu_stay_complete` (includes chartevents filtering)
3. **For research**: Document which dataset you use and justify exclusion/inclusion of orphans
4. **For data quality**: Always check `has_admission_record` flag when using complete dataset

## References

- MIMIC-III Documentation: https://mimic.mit.edu/
- Related issue: This analysis was conducted to understand the extent of orphaned ICU stays
- Date: 2025-11-27
