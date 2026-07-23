CREATE INDEX idx_stg_encounters_patient ON staging.encounters(patient);
CREATE INDEX idx_stg_encounters_start ON staging.encounters(start_ts);
CREATE INDEX idx_stg_conditions_encounter ON staging.conditions(encounter);
CREATE INDEX idx_stg_procedures_encounter ON staging.procedures(encounter);
CREATE INDEX idx_stg_claims_appointmentid ON staging.claims(appointmentid);
