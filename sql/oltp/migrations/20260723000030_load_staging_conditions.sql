INSERT INTO staging.conditions (start_dt, stop_dt, patient, encounter, code, description)
SELECT r.start_dt, r.stop_dt, r.patient, r.encounter, r.code, r.description
FROM raw.conditions r
INNER JOIN staging.encounters e ON r.encounter = e.id;