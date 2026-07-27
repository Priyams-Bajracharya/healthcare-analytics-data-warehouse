# 09 — Decision Log

This log captures the significant design and scoping decisions made during the project, in the order they were made, along with the reasoning behind each. The goal is traceability of *thinking*, not just of data — anyone reading this (including future-me) should understand why the project looks the way it does, not just what it looks like.

---

## 1. CMS DE-SynPUF claims data ruled out as a second source

**Decision**: Do not use CMS's synthetic Medicare claims (DE-SynPUF) alongside Synthea data.

**Why**: An initial design considered combining Synthea (clinical) with CMS DE-SynPUF (claims) to demonstrate multi-source integration. Before committing, the two populations' patient identifiers were checked directly — they share no common IDs, because they are two independently generated synthetic datasets. Joining them would have meant fabricating a relationship that doesn't exist in the data. Rather than force a fake join for the sake of "looking like" multi-source integration, the project uses Synthea's own native claims data instead, which is a real, verifiable relationship (see Decision 5).

---

## 2. Two separate databases (`healthcare_oltp` / `healthcare_dw`), not schema-only split

**Decision**: Use two physically separate Postgres databases — `healthcare_oltp` (containing `raw` and `staging` schemas) and `healthcare_dw` (containing the `curated` star schema) — rather than one database with three schemas.

**Why**: OLTP-shaped data (normalized, source-mirroring) and OLAP-shaped data (denormalized, query-optimized star schema) are conceptually different systems, even though this project doesn't have a live transactional system generating real-time writes. Separating them into two databases makes the OLTP → OLAP boundary concrete and demoable ("here's my source system, here's my warehouse, here's the pipeline between them"), which is a clearer story for a recruiter demo than schema prefixes inside one database. The trade-off: moving data from staging to curated requires a Python script (two DB connections) rather than a single cross-schema SQL `INSERT...SELECT`, since Postgres can't join across databases without `dblink`/`postgres_fdw`.

---

## 3. Window-start scoping: encounters filtered to `>= 2000-01-01`

**Decision**: Only encounters (and everything downstream of them) dated 2000-01-01 or later are loaded into `staging` and beyond. Data from 1912–1999 stays in `raw` (for traceability) but never reaches staging/curated.

**Why**: Synthea simulates a patient's entire life, so some patients have encounters stretching back over a century. That's realistic for a lifetime medical record, but not representative of what a hospital's analytics warehouse would typically prioritize — the project's business questions are about current operational patterns (cost trends, visit volume, caseload), not a century of history. The cutoff was chosen by inspecting the actual year-by-decade distribution of encounters (mostly concentrated from the 1990s onward) rather than picked arbitrarily; `2000-01-01` keeps roughly 72% of raw encounters while dropping the long, thin pre-2000 tail.

---

## 4. Incremental-load demo uses real 2021 data split monthly, not Faker-generated dates

**Decision**: Simulate incremental loading by treating 2000–2020 as the "initial load" and Jan–Nov 2021 as eleven monthly "incremental" batches, rather than generating synthetic new rows with Faker.

**Why**: The goal was to demonstrate the Week 4 watermark pattern advancing across multiple runs during a live demo. Using Faker to generate new rows would require fabricating internally-consistent data (new encounters referencing real patients/providers, claims summing sensibly against billed costs) — a nontrivial and risky effort given the 2-week timeline. Since the real dataset already spans through November 2021 with meaningful volume in every month (190–547 encounters/month), splitting the real data chronologically achieves the same demo goal — the watermark logic advancing batch-by-batch — using real data, at effectively zero extra engineering cost.

---

## 5. `claims_transactions.csv` verified, then descoped in favor of trimmed `claims.csv`

**Decision**: `claims_transactions.csv` was ingested into `raw` (711,238 rows) and its join to `encounters` was empirically verified, but it was excluded from `staging`/`curated`. `fact_claims` is instead sourced from a trimmed version of `claims.csv` alone.

**Why**: Before trusting any join, `claims_transactions.APPOINTMENTID` was tested against `encounters.Id` directly:
```
Encounter IDs: 61,459
Appointment IDs in transactions: 61,459
Overlap: 61,459 (100% match)
```
This confirmed Synthea's own claims data and clinical data share the same population — a real, joinable multi-source relationship, solving the same integration goal the CMS approach (Decision 1) couldn't deliver honestly. However, none of the project's 5 business questions require transaction-level detail — `encounters.TOTAL_CLAIM_COST` and `claims.diagnosis1-8` already provide sufficient financial and diagnostic detail at the encounter/claim grain. Given the project's complexity concerns and 2-week timeline, `claims_transactions` (33 columns, transaction-level grain, ~6 rows per claim) was judged to add engineering overhead without adding analytical value, so it was dropped after verification rather than before — the join is proven to work, but wasn't needed once the actual scope was clarified.

---

## 6. Column trims applied during staging (raw → staging boundary)

**Decision**: The `raw` schema retains every column from every source CSV, unmodified, to satisfy the project's traceability requirement. Column drops and simplifications only happen at the `raw` → `staging` boundary, explicitly and one time, not scattered across the pipeline.

**Columns dropped, by category and reason**:

| Category | Columns | Reason |
|---|---|---|
| PII / name formatting | `patients.ssn`, `drivers`, `passport`, `prefix`, `suffix`, `maiden` | Not needed for any business question; synthetic PII with no analytical value |
| Unmatched insurer references | `encounters.payer`, `claims.primarypatientinsuranceid`, `secondarypatientinsuranceid`, `claims_transactions.patientinsuranceid` | `payers.csv` was never in scope, so these UUIDs point to nothing in the model |
| AR / billing-cycle tracking | `claims.status1/2/p`, `outstanding1/2/p`, `lastbilleddate1/2/p`, `healthcareclaimtypeid1/2` | Models insurer billing-cycle status, not relevant to any of the 5 target questions |
| Dead columns (100% null in source) | `claims.referringproviderid`, `claims_transactions.modifier1/2`, `adjustments`, `linenote` | Confirmed null across all rows during exploration |
| Geographic/contact detail | `organizations.address/zip/phone/lat/lon`, `providers.address/city/state/zip/lat/lon/gender`, `patients.deathdate/marital/birthplace/address/city/state/county/zip/lat/lon` | Not used by any business question; street-level/geo detail adds no value at this project's grain |
| Precomputed redundant aggregates | `organizations.utilization`, `providers.utilization`, `patients.healthcare_expenses/healthcare_coverage` | Redundant with values computable directly from `fact_encounters`; keeping both risks the two disagreeing |
| Encounter/procedure timing & cost detail | `encounters.base_encounter_cost`, `payer_coverage`, `reasoncode/reasondescription`; `procedures.start_ts/stop_ts`, `reasoncode/reasondescription` | `total_claim_cost` alone answers the cost questions; reason codes were ~75% null and unused |

One column (`patients.birthdate`) was kept despite not being directly used by the 5 core questions, since it's cheap to retain and enables age-based analysis later without needing to re-ingest from raw.

**Still open**: `organizations.revenue` — appeared to be `0` across sampled rows; pending a `SELECT DISTINCT revenue FROM raw.organizations` check to confirm before deciding whether to drop it from staging too.

---

## Format for future entries

New decisions should follow the same shape: **Decision** (what was chosen), **Why** (the reasoning, including what was ruled out and why). Entries are numbered in chronological order and never renumbered or removed, even if a later decision reverses an earlier one — the log is a history, not just a current-state summary.

## Decision: encounters.code/description kept as degenerate dimension
Date: 2026-07-25
Context: 49 distinct encounter type codes found in staging.encounters. Not tiny, but none
of the 5 locked business questions require breakdown by encounter type.
Decision: keep code/description as plain columns on fact_encounters (degenerate dimension),
not promoted to a separate dim_encounter_type.
Revisit: if a future question needs encounter-type breakdown, promote to its own dim then.

## Finding: duration_minutes outliers in outpatient encounters (code 185347001)
Date: 2026-07-25
A subset of "Encounter for problem (procedure)" outpatient encounters show implausible
durations (300k-530k minutes, ~10-12 months) — likely a Synthea data generation artifact,
not realistic clinical outpatient visits.
Decision: keep duration_minutes as a stored measure for completeness, but flag that
duration-based averages/aggregates should be filtered or interpreted with this caveat in mind.
Not fixed/excluded since none of the 5 locked business questions rely on average visit duration.