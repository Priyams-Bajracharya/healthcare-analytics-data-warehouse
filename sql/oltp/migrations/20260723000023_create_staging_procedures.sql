CREATE TABLE staging.procedures (
    row_id        BIGSERIAL PRIMARY KEY,
    patient       UUID REFERENCES staging.patients(id),
    encounter     UUID REFERENCES staging.encounters(id),
    code          TEXT,
    description   TEXT,
    base_cost     NUMERIC(14,2)
);