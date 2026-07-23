INSERT INTO staging.providers (id, organization, name, speciality)
SELECT id, organization, name, speciality
FROM raw.providers;
