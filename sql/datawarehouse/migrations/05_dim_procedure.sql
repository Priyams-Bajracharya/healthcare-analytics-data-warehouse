-- dim_procedure: one row per DISTINCT procedure code (deduplicated from staging.procedures)
-- Linked to fact_encounters via bridge_encounter_procedure (many-to-many: one encounter
-- can have up to 27 procedures, per empirical check against staging.procedures).
-- base_cost is NOT stored here — confirmed it varies per occurrence (same procedure code
-- has multiple distinct costs), so it lives on the bridge table instead.
CREATE TABLE dim_procedure (
    procedure_key serial PRIMARY KEY,
    code          text NOT NULL,
    description   text NOT NULL,
    CONSTRAINT uq_procedure_code UNIQUE (code)
);
