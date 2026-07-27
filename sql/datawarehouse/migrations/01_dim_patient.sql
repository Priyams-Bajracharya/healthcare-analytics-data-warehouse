-- dim_patient: one row per patient
CREATE TABLE dim_patient (
    patient_key      serial PRIMARY KEY,              -- surrogate key
    source_patient_id uuid  NOT NULL,                  -- business key (traceability to raw/staging)
    birthdate        date,
    CONSTRAINT uq_patient_source_id UNIQUE (source_patient_id)
);
