-- dim_diagnosis: one row per DISTINCT diagnosis code (deduplicated from staging.conditions)
-- Linked to fact_encounters via bridge_encounter_diagnosis (many-to-many: one encounter
-- can have up to 13 diagnoses, per empirical check against staging.conditions).
CREATE TABLE dim_diagnosis (
    diagnosis_key serial PRIMARY KEY,
    code          text NOT NULL,
    description   text NOT NULL,
    CONSTRAINT uq_diagnosis_code UNIQUE (code)
);
