# ICU Scripts Item ID Review - Pending Analysis

**Status:** Second batch of systematic item ID review completed
**Last Updated:** 2025-11-28
**Reviewed By:** Comprehensive analysis against MIMIC-III standard patterns

---

## ✅ Completed Reviews (All KEEP AS-IS)

### Vitals
- [x] icu_age_duckdb.sql - No item/lab IDs (DOB calculation only)
- [x] icu_first_hr_duckdb.sql - 2 IDs (211, 220045) - Perfect alignment
- [x] icu_first_bp_duckdb.sql - 6 IDs each (SBP/DBP) - Perfect alignment
- [x] icu_first_resprate_duckdb.sql - 4 IDs (615, 618, 220210, 224690) - Perfect alignment

### Biomarkers & Labs (Routine)
- [x] icu_first_creatinine_duckdb.sql - 1 ID (50912) - Perfect alignment
- [x] icu_first_bun_duckdb.sql - 1 ID (51006) - Perfect alignment
- [x] icu_first_potassium_duckdb.sql - 2 IDs (50971, 50822) - Dual sources optimal

### Biomarkers & Labs (Sparse)
- [x] icu_first_ntprobnp_duckdb.sql - 1 ID (50963) - Closest extraction optimal
- [x] icu_first_total_cholesterol_duckdb.sql - 1 ID (50907) - Closest extraction optimal

---

## ⏳ Pending Reviews (Future Iterations)

### Temperature & Oxygen
- [ ] icu_first_temperature_duckdb.sql - 4 IDs with unit conversion
  - Decision needed: F-to-C conversion approach consistency

- [ ] icu_first_spo2_duckdb.sql (if separate) - Item ID count TBD

### Remaining Lab Values
- [ ] icu_first_hemoglobin_duckdb.sql - 2 IDs (51222, 50811) - Dual sources expected
  - Verify: Matches potassium dual-source pattern

- [ ] icu_first_hematocrit_duckdb.sql - 2 IDs (51221, 50810) - Dual sources expected
  - Verify: Matches potassium dual-source pattern

- [ ] icu_first_wbc_duckdb.sql - Item ID count TBD
  - Verify: Single vs dual sources

- [ ] icu_first_platelet_duckdb.sql - Item ID count TBD
  - Verify: Single source expected

### Lipid Panel Components
- [ ] icu_first_ldl_duckdb.sql - Item ID count TBD
  - Verify: Sparse vs routine classification
  - Verify: Closest vs earliest extraction logic
  - Verify: Consistency with total_cholesterol approach

- [ ] icu_first_hdl_duckdb.sql - Item ID count TBD
  - Verify: Sparse vs routine classification
  - Verify: Closest vs earliest extraction logic

- [ ] icu_first_triglycerides_duckdb.sql - Item ID count TBD
  - Verify: Sparse vs routine classification
  - Verify: Closest vs earliest extraction logic

### Anthropometrics & Rhythm
- [ ] icu_first_height_duckdb.sql - 8 IDs with unit conversion expected
  - Decision needed: Upper bound appropriateness (current: 230 cm)

- [ ] icu_first_weight_duckdb.sql - 4 IDs with priority hierarchy expected
  - Decision needed: 2-level vs 4-level hierarchy (current: simplified)
  - Verify: Echo weight as fallback inclusion

- [ ] icu_first_rhythm_duckdb.sql - Item ID count TBD
  - New measurement type - classification needed

### Foundational/Composite Tables
- [ ] icu_first_vitals_duckdb.sql - Aggregated vitals in single table
  - Verify: Consistency with individual vital scripts

- [ ] icustay_detail_duckdb.sql - Demographic/administrative data
  - Review: Item ID relevance (may have none)

- [ ] icu_primary_diagnosis_duckdb.sql - Diagnosis extraction
  - Review: ICD-9 code patterns vs item IDs

- [ ] icu_readmission_30d_duckdb.sql - Readmission flags
  - Review: Item ID relevance (likely none)

- [ ] icu_stay_complete_duckdb.sql - Comprehensive cohort table
  - Review: Proper joining and aggregation of all components

- [ ] icu_stay_complete_with_orphans_duckdb.sql - Extended version
  - Review: Orphan handling approach

---

## Key Questions for Future Reviews

### Time Window Consistency
- [ ] Verify all "sparse" labs use ±7 day window
- [ ] Verify all "routine" labs use ±6 hour window
- [ ] Check if any labs need reclassification

### Extraction Logic
- [ ] Verify all sparse labs use "closest by absolute distance"
- [ ] Verify all routine labs use "earliest chronologically"
- [ ] Identify any outliers needing justification

### Unit Conversion
- [ ] Audit all unit conversions (temperature F→C, height inches→cm)
- [ ] Ensure output units are standardized and documented
- [ ] Check for any unit handling inconsistencies

### Dual vs Single Source
- [ ] Confirm dual sources follow potassium model (both included equally)
- [ ] Identify any labs with unnecessary multiple sources
- [ ] Identify any labs missing appropriate dual sources

### Data Completeness vs Clarity
- [ ] Weight hierarchy: Keep simplified or restore 4-level?
- [ ] Glucose (vitals): Add 226537 (whole blood) or document exclusion?
- [ ] Any other completeness vs simplicity trade-offs?

---

## Findings Summary (Completed Round)

**Overall Assessment:** Scripts are well-optimized with thoughtful design choices.

**Key Patterns Identified:**
1. **Routine labs** (creatinine, BUN, potassium): Earliest extraction, ±6h window
2. **Sparse labs** (NT-proBNP, cholesterol): Closest extraction, ±7d window
3. **Dual sources** (potassium, hemoglobin): Both included equally by timestamp
4. **Single sources** (creatinine, BUN): Appropriate for chemistry-only measurements
5. **Administrative times:** Consistent use of icustays intime/outtime (pragmatic approach)

**Changes Recommended:** NONE - all reviewed scripts are KEEP AS-IS

**Documentation Quality:** Excellent - comprehensive comments and clinical context

---

## Review Order (Suggested for Future Rounds)

**Quick wins (minimal complexity):**
1. icu_first_platelet_duckdb.sql - Expected single source
2. icu_first_rhythm_duckdb.sql - New measurement type
3. icu_primary_diagnosis_duckdb.sql - ICD codes, not item IDs

**Medium complexity:**
4. icu_first_hemoglobin_duckdb.sql - Dual sources (pattern known)
5. icu_first_hematocrit_duckdb.sql - Dual sources (pattern known)
6. icu_first_wbc_duckdb.sql - TBD dual/single
7. icu_first_temperature_duckdb.sql - Unit conversion verification

**Higher complexity:**
8. icu_first_ldl_duckdb.sql - Lipid classification needed
9. icu_first_hdl_duckdb.sql - Lipid classification needed
10. icu_first_triglycerides_duckdb.sql - Lipid classification needed
11. icu_first_weight_duckdb.sql - Hierarchy decision needed
12. icu_first_height_duckdb.sql - Unit bound decision needed

**Foundational tables (final):**
13. icu_first_vitals_duckdb.sql - Aggregation verification
14. icustay_detail_duckdb.sql - Demographic verification
15. icu_readmission_30d_duckdb.sql - Flag logic verification
16. icu_stay_complete_*.sql - Final integration verification

---

**Notes for Next Session:**
- Start with quick wins (platelet, rhythm, diagnosis)
- Apply "routine vs sparse" pattern to lipid panel
- Resolve weight hierarchy and height bounds questions
- Final verification of foundational tables
- Estimate: ~2-3 hours for remaining items
