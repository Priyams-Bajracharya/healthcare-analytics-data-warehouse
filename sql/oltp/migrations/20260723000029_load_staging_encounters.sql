INSERT INTO staging.encounters (id, start_ts, stop_ts, patient, organization, provider, encounterclass, code, description, total_claim_cost)
SELECT id, start_ts, stop_ts, patient, organization, provider,encounterclass, code, description, total_claim_cost
FROM raw.encounters
WHERE start_ts >= '2000-01-01T00:00:00Z';