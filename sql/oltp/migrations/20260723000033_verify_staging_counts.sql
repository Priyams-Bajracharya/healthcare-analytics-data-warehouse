SELECT 'organizations' AS tbl, COUNT(*) FROM staging.organizations
UNION ALL SELECT 'providers', COUNT(*) FROM staging.providers
UNION ALL SELECT 'patients', COUNT(*) FROM staging.patients
UNION ALL SELECT 'encounters', COUNT(*) FROM staging.encounters
UNION ALL SELECT 'conditions', COUNT(*) FROM staging.conditions
UNION ALL SELECT 'procedures', COUNT(*) FROM staging.procedures
UNION ALL SELECT 'claims', COUNT(*) FROM staging.claims;