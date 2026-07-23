INSERT INTO staging.patients (id, birthdate)
SELECT id, birthdate
FROM raw.patients;