# Healthcare Analytics Data Warehouse
## Problem Statement & Raw Data Documentation

---

## 1. Problem Statement

### Background
Healthcare organizations generate data across multiple systems — patient records, clinical encounters, diagnoses, procedures, and insurance billing — but this data is often fragmented across disconnected systems. This fragmentation makes it difficult for hospital administrators, analysts, and decision-makers to answer basic operational and financial questions without manually reconciling data from multiple sources.

### Goal
Build an end-to-end data engineering pipeline that ingests, cleans, models, and integrates clinical and financial healthcare data into a single analytics-ready warehouse, enabling self-service analysis through a business intelligence dashboard.

### Business Questions This Project Must Answer
1. What are the highest healthcare costs by diagnosis?
2. Which procedures generate the most expense?
3. How many patients are being treated over time (visit volume trends)?
4. Which providers/organizations handle the most cases (caseload)?
**Stretch (nice-to-have, time permitting):**
5. Cost per encounter by provider/organization (efficiency lens)

### Non-Functional Requirements
- **Traceability**: every row in the warehouse must be traceable back to its raw source record
- **Idempotency**: the pipeline must be safely re-runnable without duplicating or corrupting data
- **Incremental capability**: new data should be loadable without a full reprocess of the entire dataset
- **Visibility**: failures must be logged and diagnosable, not silent

### Explicitly Out of Scope
- Real-time/streaming ingestion (data is batch, matching how real hospital/claims data is typically produced)
- External CMS claims data — an initial design considered CMS's synthetic Medicare claims (DE-SynPUF) as a second, complementary financial data source, but this was ruled out after confirming CMS's synthetic patient population has no shared identifiers with Synthea's population (they are independently generated synthetic datasets). Rather than force a fake join, the project uses Synthea's own native claims data, which shares the same patient/encounter population and has a verified referential link (see Section 3).
- Cloud deployment, distributed processing (Spark), or object storage (S3/MinIO) — noted as future-work directions, not built in this version
- PHI/HIPAA compliance controls — not applicable since all data is synthetic, but acknowledged as a real production requirement outside this project's scope

---

## 2. Data Source Overview

**Source**: [Synthea](https://synthea.mitre.org/) — an open-source synthetic patient population simulator that generates realistic (but entirely fictional) electronic health record data, including clinical events and native billing/claims data.

**Population size**: 1,163 synthetic patients (a standard pre-built Synthea sample export)

**Format**: CSV files, one file per entity type

**Why Synthea**: it produces internally consistent data — patients, their encounters, diagnoses, procedures, and billing claims are all generated from the same simulation and share real, verifiable identifiers across files. This was confirmed empirically (see Section 3) before committing to the data model.

---

## 3. Raw Data Description

The pipeline ingests 8 of Synthea's output files. Each is described below with its grain (what one row represents), row count, and role in the project.

### `patients.csv`
- **Grain**: one row per person
- **Rows**: 1,163
- **Columns**: 25 (includes `Id`, `BIRTHDATE`, `DEATHDATE`, `GENDER`, `RACE`, `ETHNICITY`, address fields)
- **Role**: source for `dim_patient`

### `encounters.csv`
- **Grain**: one row per clinical visit/encounter
- **Rows**: 61,459
- **Key columns**: `Id`, `START`, `STOP`, `PATIENT` (→ patients.Id), `ORGANIZATION`, `PROVIDER`, `PAYER`, `ENCOUNTERCLASS`, `CODE`, `DESCRIPTION`, `BASE_ENCOUNTER_COST`, `TOTAL_CLAIM_COST`, `PAYER_COVERAGE`, `REASONCODE`, `REASONDESCRIPTION`
- **Role**: source for `fact_encounters` — the clinical fact table

### `conditions.csv`
- **Grain**: one row per diagnosis recorded during an encounter
- **Rows**: 38,094
- **Key columns**: `PATIENT`, `ENCOUNTER` (→ encounters.Id), `CODE`, `DESCRIPTION`
- **Role**: source for diagnosis detail, feeds `dim_diagnosis`

### `procedures.csv`
- **Grain**: one row per procedure performed during an encounter
- **Rows**: 83,823
- **Key columns**: `PATIENT`, `ENCOUNTER` (→ encounters.Id), `CODE`, `DESCRIPTION`, cost fields
- **Role**: source for procedure detail, feeds `dim_procedure`

### `providers.csv`
- **Grain**: one row per clinician/provider
- **Rows**: 5,056
- **Role**: source for `dim_provider`

### `organizations.csv`
- **Grain**: one row per hospital/clinic/organization
- **Rows**: 1,127
- **Role**: source for `dim_organization`

### `claims.csv`
- **Grain**: one row per billing claim (a claim "header" — one claim can cover multiple diagnoses)
- **Rows**: 117,889
- **Key columns**: `Id`, `PATIENTID` (→ patients.Id), `PROVIDERID`, `DIAGNOSIS1`–`DIAGNOSIS8`, `SERVICEDATE`, billing status fields (`OUTSTANDING1/2/P`, `LASTBILLEDDATE1/2/P`)
- **Role**: billing header data, source for `fact_claims`


### Files intentionally excluded from scope
`medications.csv`, `observations.csv`, `immunizations.csv`, `allergies.csv`, `careplans.csv`, `imaging_studies.csv`, `devices.csv`, `payers.csv`, `payer_transitions.csv`, `supplies.csv`,`claims_transactions.csv` —— the first ten exist in the Synthea export but were never in scope; `claims_transactions.csv` was initially ingested into the raw layer and evaluated (see Section 4) but excluded from staging/curated to keep the model's complexity proportionate to the project's 2-week timeline.
These exist in the Synthea export but are not required to answer the project's target business questions. Excluded deliberately to keep scope focused, not because they were overlooked.

---

## 4. Verified Data Relationships

Two join paths were tested empirically before being trusted, rather than assumed:

**1. Synthea vs. CMS DE-SynPUF (ruled out)** — confirmed the two synthetic patient populations share no common identifiers, so a CMS+Synthea join would have been fabricated, not real. This is why external CMS data was dropped from scope (see Section 1).

**2. `claims_transactions.APPOINTMENTID` vs. `encounters.Id` (tested, then descoped)** — this join was verified at 100% overlap (61,459 = 61,459 = 61,459) during initial exploration, confirming Synthea's claims data and clinical data share the same population and are genuinely joinable. `claims_transactions.csv` was later excluded from the final model to reduce complexity (see decision log), but the verification stands as evidence the data supports multi-source integration if extended in future work.

**3. `claims.appointmentid` vs. `encounters.Id` (in production use)** — this is the join actually used in the final model, connecting `fact_claims` to `fact_encounters` at the encounter level:

This confirms `claims_transactions.APPOINTMENTID` is equivalent to `encounters.Id` — Synthea uses "appointment" and "encounter" as synonymous identifiers across these two files. This gives a fully verified join path connecting the clinical and financial sides of the data:

```
patients ← encounters ← conditions
                ↕            (via ENCOUNTER)
                claims_transactions ← claims
         (via APPOINTMENTID = encounters.Id)   (via CLAIMID = claims.Id)
```

This verification step — confirming referential integrity empirically before building the pipeline — is treated as a project requirement, not an assumption.

---

## 5. Enrichment

Diagnosis and procedure codes in the raw data (`conditions.CODE`, `procedures.CODE`) are enriched with standardized descriptions via a live call to the NLM Clinical Tables API (ICD-10/CPT code lookup), feeding into `dim_diagnosis` and `dim_procedure`. This demonstrates ingestion from an external API source, not just static file loading.