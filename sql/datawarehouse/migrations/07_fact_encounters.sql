-- ============================================================================
-- SECTION 2: FACT TABLE
-- Naming convention: fact tables are plural (fact_encounters) — represents
-- a log of many encounter events.
-- Grain: one row per encounter.
-- ============================================================================

CREATE TABLE fact_encounters (
    encounter_key       serial PRIMARY KEY,
    source_encounter_id uuid NOT NULL,                 -- business key (traceability)

    -- dimension FKs
    patient_key      int  NOT NULL REFERENCES dim_patient(patient_key),
    provider_key     int  NOT NULL REFERENCES dim_provider(provider_key),
    organization_key int  NOT NULL REFERENCES dim_organization(organization_key),
    date_key         date NOT NULL REFERENCES dim_date(date_key),   -- encounter START date

    -- degenerate dimensions: kept as plain columns, not promoted to their own dim table.
    -- encounterclass: broad setting category (ambulatory/emergency/inpatient/etc).
    -- code/description: specific visit type (49 distinct values found in staging.encounters —
    --   not tiny, but none of the 5 locked business questions require breakdown by encounter
    --   type, so kept degenerate for now. See decision log; can promote to dim_encounter_type
    --   later if a future question needs it).
    encounterclass text,
    code           text,
    description    text,

    -- measures
    total_claim_cost numeric(10,2) NOT NULL,   -- whole-encounter cost (min $0, max $625,902.71)
    duration_minutes integer,                   -- derived: EXTRACT(EPOCH FROM (stop_ts-start_ts))/60
                                                 -- NOTE: some outpatient encounters (code 185347001)
                                                 -- show implausible durations (300k-530k minutes,
                                                 -- ~10-12 months) — likely a Synthea data generation
                                                 -- artifact. Kept for completeness; see decision log —
                                                 -- duration-based averages should be filtered/interpreted
                                                 -- with this caveat. Not fixed since no locked business
                                                 -- question depends on average visit duration.

    -- raw timestamps kept for precision / duration recomputation (not FK'd to dim_date,
    -- since only the start date is needed for the "visit volume trends" business question)
    start_ts timestamp NOT NULL,
    stop_ts  timestamp NOT NULL
);
