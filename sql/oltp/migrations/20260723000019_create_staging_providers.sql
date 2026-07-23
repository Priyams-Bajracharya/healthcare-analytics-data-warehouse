CREATE TABLE staging.providers (
    id            UUID PRIMARY KEY,
    organization  UUID REFERENCES staging.organizations(id),
    name          TEXT,
    speciality    TEXT
);
