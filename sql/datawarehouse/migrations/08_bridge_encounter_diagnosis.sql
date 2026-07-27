-- ============================================================================
-- SECTION 3: BRIDGE TABLES
-- Resolve many-to-many relationships between fact_encounters and
-- dim_diagnosis / dim_procedure, without duplicating fact_encounters rows
-- (which would corrupt SUM/AVG on total_claim_cost).
-- Composite PK chosen over a surrogate key because empirical checks confirmed
-- (encounter, code) pairs are always unique in staging.conditions / staging.procedures.
-- ============================================================================

CREATE TABLE bridge_encounter_diagnosis (
    encounter_key int NOT NULL REFERENCES fact_encounters(encounter_key),
    diagnosis_key int NOT NULL REFERENCES dim_diagnosis(diagnosis_key),
    PRIMARY KEY (encounter_key, diagnosis_key)
);
