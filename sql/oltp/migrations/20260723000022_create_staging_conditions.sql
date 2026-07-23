CREATE TABLE staging.conditions (
    row_id        BIGSERIAL PRIMARY KEY,
    start_dt      DATE,
    stop_dt       DATE,
    patient       UUID REFERENCES staging.patients(id),
    encounter     UUID REFERENCES staging.encounters(id),
    code          TEXT,
    description   TEXT
);
