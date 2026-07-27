-- base_cost lives here (not on dim_procedure) because it's a per-occurrence measure:
-- the same procedure code has different costs across different encounters.
CREATE TABLE bridge_encounter_procedure (
    encounter_key int NOT NULL REFERENCES fact_encounters(encounter_key),
    procedure_key int NOT NULL REFERENCES dim_procedure(procedure_key),
    base_cost     numeric(10,2),
    PRIMARY KEY (encounter_key, procedure_key)
);
