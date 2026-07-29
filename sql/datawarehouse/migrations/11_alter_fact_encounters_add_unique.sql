-- Adds UNIQUE constraint on source_encounter_id, needed for ON CONFLICT
-- in load_fact_encounters (etl/load.py) — enables incremental idempotency
ALTER TABLE fact_encounters
ADD CONSTRAINT uq_fact_encounters_source_encounter_id UNIQUE (source_encounter_id);