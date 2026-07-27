-- dim_provider: one row per provider
-- Note: no FK to dim_organization here — dimensions stay "flat" in a star schema.
-- fact_encounters links to organization directly instead.
CREATE TABLE dim_provider (
    provider_key      serial PRIMARY KEY,
    source_provider_id uuid NOT NULL,
    name              text,
    speciality        text,
    CONSTRAINT uq_provider_source_id UNIQUE (source_provider_id)
);
