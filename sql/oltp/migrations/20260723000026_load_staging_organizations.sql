INSERT INTO staging.organizations (id, name, city, state)
SELECT id, name, city, state
FROM raw.organizations;