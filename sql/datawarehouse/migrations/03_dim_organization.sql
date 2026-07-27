-- dim_organization: one row per organization
CREATE TABLE dim_organization (
    organization_key      serial PRIMARY KEY,
    source_organization_id uuid NOT NULL,
    name                  text,
    city                  text,
    state                 text,
    CONSTRAINT uq_organization_source_id UNIQUE (source_organization_id)
);
