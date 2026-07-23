-- =====================================================================
-- healthcare_oltp database — RAW schema
-- Mirrors the 8 Synthea source CSVs 1:1 (all columns, no drops)
-- Purpose: traceability / audit trail — every warehouse row must trace
-- back to a raw record here. No cleaning happens at this layer.
-- =====================================================================

-- Run this AFTER creating the database:
--   CREATE DATABASE healthcare_oltp;
-- (CREATE DATABASE can't run inside a transaction / this script — run
-- it separately first, then connect to healthcare_oltp and run this file)

CREATE SCHEMA IF NOT EXISTS raw;

-- ---------------------------------------------------------------------
-- raw.organizations  (load before providers — providers FKs to this)
-- ---------------------------------------------------------------------
CREATE TABLE raw.organizations (
    id            UUID PRIMARY KEY,
    name          TEXT,
    address       TEXT,
    city          TEXT,
    state         TEXT,
    zip           TEXT,          -- text, not int: leading zeros + nulls
    lat           NUMERIC(9,6),
    lon           NUMERIC(9,6),
    phone         TEXT,
    revenue       NUMERIC(14,2), -- flagged: appears to be always 0, confirm before dropping in staging
    utilization   INTEGER
);

-- ---------------------------------------------------------------------
-- raw.providers  (load before encounters — encounters FKs to this)
-- ---------------------------------------------------------------------
CREATE TABLE raw.providers (
    id            UUID PRIMARY KEY,
    organization  UUID REFERENCES raw.organizations(id),
    name          TEXT,
    gender        TEXT,
    speciality    TEXT,          -- source spelling kept as-is for traceability
    address       TEXT,
    city          TEXT,
    state         TEXT,
    zip           TEXT,
    lat           NUMERIC(9,6),
    lon           NUMERIC(9,6),
    utilization   INTEGER
);

-- ---------------------------------------------------------------------
-- raw.patients
-- ---------------------------------------------------------------------
CREATE TABLE raw.patients (
    id                    UUID PRIMARY KEY,
    birthdate             DATE,
    deathdate             DATE,
    ssn                   TEXT,
    drivers               TEXT,
    passport              TEXT,
    prefix                TEXT,
    first                 TEXT,
    last                  TEXT,
    suffix                TEXT,
    maiden                TEXT,
    marital               TEXT,
    race                  TEXT,
    ethnicity             TEXT,
    gender                TEXT,
    birthplace            TEXT,
    address               TEXT,
    city                  TEXT,
    state                 TEXT,
    county                TEXT,
    zip                   TEXT,
    lat                   NUMERIC(9,6),
    lon                   NUMERIC(9,6),
    healthcare_expenses   NUMERIC(14,4),
    healthcare_coverage   NUMERIC(14,4)
);

-- ---------------------------------------------------------------------
-- raw.encounters
-- ---------------------------------------------------------------------
CREATE TABLE raw.encounters (
    id                     UUID PRIMARY KEY,
    start_ts               TIMESTAMPTZ,   -- START is reserved-ish, use start_ts
    stop_ts                TIMESTAMPTZ,
    patient                UUID REFERENCES raw.patients(id),
    organization            UUID REFERENCES raw.organizations(id),
    provider                UUID REFERENCES raw.providers(id),
    payer                   UUID,          -- NO FK: payers.csv intentionally excluded from scope
    encounterclass           TEXT,
    code                    TEXT,
    description             TEXT,
    base_encounter_cost      NUMERIC(14,2),
    total_claim_cost         NUMERIC(14,2),
    payer_coverage           NUMERIC(14,2),
    reasoncode               TEXT,
    reasondescription         TEXT
);

-- ---------------------------------------------------------------------
-- raw.conditions  (no natural PK in source — synthetic surrogate added)
-- ---------------------------------------------------------------------
CREATE TABLE raw.conditions (
    row_id        BIGSERIAL PRIMARY KEY,   -- surrogate; source has no Id column
    start_dt      DATE,
    stop_dt       DATE,                    -- null = condition still ongoing
    patient       UUID REFERENCES raw.patients(id),
    encounter     UUID REFERENCES raw.encounters(id),
    code          TEXT,
    description   TEXT
);

-- ---------------------------------------------------------------------
-- raw.procedures  (no natural PK in source — synthetic surrogate added)
-- ---------------------------------------------------------------------
CREATE TABLE raw.procedures (
    row_id               BIGSERIAL PRIMARY KEY,
    start_ts             TIMESTAMPTZ,
    stop_ts              TIMESTAMPTZ,
    patient              UUID REFERENCES raw.patients(id),
    encounter            UUID REFERENCES raw.encounters(id),
    code                 TEXT,
    description          TEXT,
    base_cost            NUMERIC(14,2),
    reasoncode           TEXT,
    reasondescription    TEXT
);

-- ---------------------------------------------------------------------
-- raw.claims
-- ---------------------------------------------------------------------
CREATE TABLE raw.claims (
    id                             UUID PRIMARY KEY,
    patientid                      UUID REFERENCES raw.patients(id),
    providerid                     UUID REFERENCES raw.providers(id),
    primarypatientinsuranceid      UUID,   -- NO FK: payers.csv excluded from scope
    secondarypatientinsuranceid    UUID,   -- NO FK: same reason
    departmentid                   INTEGER,
    patientdepartmentid            INTEGER,
    diagnosis1                     TEXT,
    diagnosis2                     TEXT,
    diagnosis3                     TEXT,
    diagnosis4                     TEXT,
    diagnosis5                     TEXT,
    diagnosis6                     TEXT,
    diagnosis7                     TEXT,
    diagnosis8                     TEXT,
    referringproviderid            UUID,   -- 100% null in source, kept for fidelity
    appointmentid                  UUID REFERENCES raw.encounters(id),
    currentillnessdate             DATE,
    servicedate                    DATE,
    supervisingproviderid          UUID REFERENCES raw.providers(id),
    status1                        TEXT,
    status2                        TEXT,
    statusp                        TEXT,
    outstanding1                   NUMERIC(14,2),
    outstanding2                   NUMERIC(14,2),
    outstandingp                   NUMERIC(14,2),
    lastbilleddate1                DATE,
    lastbilleddate2                DATE,
    lastbilleddatep                DATE,
    healthcareclaimtypeid1         INTEGER,
    healthcareclaimtypeid2         INTEGER
);

-- ---------------------------------------------------------------------
-- raw.claims_transactions
-- ---------------------------------------------------------------------
CREATE TABLE raw.claims_transactions (
    id                       UUID PRIMARY KEY,
    claimid                  UUID REFERENCES raw.claims(id),
    chargeid                 INTEGER,       -- line number within the claim
    patientid                UUID REFERENCES raw.patients(id),
    type                     TEXT,
    amount                   NUMERIC(14,2),
    method                   TEXT,
    fromdate                 DATE,
    todate                   DATE,
    placeofservice           TEXT,
    procedurecode             TEXT,
    modifier1                TEXT,          -- 100% null in source, kept for fidelity
    modifier2                TEXT,          -- 100% null in source, kept for fidelity
    diagnosisref1             SMALLINT,      -- index (1-8) into parent claims.DIAGNOSIS#, not a code itself
    diagnosisref2             SMALLINT,
    diagnosisref3             SMALLINT,
    diagnosisref4             SMALLINT,
    units                    INTEGER,
    departmentid              INTEGER,
    notes                    TEXT,
    unitamount                NUMERIC(14,2),
    transferoutid             UUID,
    transfertype              TEXT,
    payments                 NUMERIC(14,2),
    adjustments               NUMERIC(14,2), -- 100% null in source, kept for fidelity
    transfers                NUMERIC(14,2),
    outstanding               NUMERIC(14,2),
    appointmentid             UUID REFERENCES raw.encounters(id),  -- VERIFIED 100% join key to encounters.id
    linenote                 TEXT,           -- 100% null in source, kept for fidelity
    patientinsuranceid        UUID,           -- NO FK: payers.csv excluded from scope
    feescheduleid             INTEGER,
    providerid                UUID REFERENCES raw.providers(id),
    supervisingproviderid     UUID REFERENCES raw.providers(id)
);

-- ---------------------------------------------------------------------
-- Indexes on FK columns used for the incremental split / staging joins
-- (raw layer is bulk-loaded once per batch, so keep indexing light —
-- just what's needed to support the staging layer's SELECTs)
-- ---------------------------------------------------------------------
CREATE INDEX idx_encounters_patient ON raw.encounters(patient);
CREATE INDEX idx_encounters_start ON raw.encounters(start_ts);   -- incremental watermark column
CREATE INDEX idx_conditions_encounter ON raw.conditions(encounter);
CREATE INDEX idx_procedures_encounter ON raw.procedures(encounter);
CREATE INDEX idx_claims_appointmentid ON raw.claims(appointmentid);
CREATE INDEX idx_claims_txn_claimid ON raw.claims_transactions(claimid);
CREATE INDEX idx_claims_txn_appointmentid ON raw.claims_transactions(appointmentid);