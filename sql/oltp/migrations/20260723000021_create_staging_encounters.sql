CREATE TABLE staging.encounters (
    id                  UUID PRIMARY KEY,
    start_ts            TIMESTAMPTZ NOT NULL,
    stop_ts             TIMESTAMPTZ,
    patient             UUID REFERENCES staging.patients(id),
    organization        UUID REFERENCES staging.organizations(id),
    provider            UUID REFERENCES staging.providers(id),
    encounterclass      TEXT,
    code                TEXT,
    description         TEXT,
    total_claim_cost    NUMERIC(14,2)
);
