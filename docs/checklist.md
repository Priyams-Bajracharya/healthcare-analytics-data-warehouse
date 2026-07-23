# Healthcare Data Lakehouse — Production Requirements Checklist

**Project**: Healthcare Analytics Data Warehouse (Synthea EHR + native claims data)
**Stack**: Python, PostgreSQL, Airflow, Power BI
**Purpose**: Track production-level engineering discipline against everything covered in the bootcamp, so the final project demonstrates real practices — not just a working script.

---

## 1. Data Ingestion
- [ ] Ingest all raw CSVs into a `raw` schema, unmodified (no transformation at this stage)
- [ ] Ingestion is script-based (Python), not manual CSV imports — repeatable, not a one-off
- [ ] Credentials (DB connection) loaded via `.env` / `python-dotenv`, never hardcoded
- [ ] Ingestion uses a transactional loader pattern (`load_batch()` style) — commit per batch, not one giant uncommitted transaction

## 2. Data Cleaning & Standardization
- [ ] Null handling documented and applied consistently (`COALESCE`, explicit "unknown" categories where appropriate)
- [ ] Type casting enforced (dates as real `DATE`/`TIMESTAMP`, costs as `NUMERIC`, not text)
- [ ] Code/text standardization (e.g., `INITCAP` or consistent casing on categorical fields)
- [ ] Deduplication logic applied and justified (even if zero duplicates found, document that it was checked)

## 3. Data Modeling (Kimball Star Schema)
- [ ] Explicit grain statement for each fact table (one row = ?), written down
- [x] Two-database architecture: `healthcare_oltp` (raw + staging schemas) and `healthcare_dw` (curated schema) — not a single-database schema split, since raw/staging are OLTP-shaped and curated is OLAP-shaped
- [ ] Star schema: `fact_encounters`, `fact_claims` + shared `dim_patient`, `dim_provider`, `dim_organization`, `dim_diagnosis`, `dim_procedure`, `dim_date`
- [ ] Foreign key constraints actually enforced in Postgres (not just implied by naming)
- [ ] At least one enriched dimension via external API (ICD-10/CPT descriptions)

## 4. Incremental Loading
- [ ] Control/tracking table storing the max timestamp or ID successfully loaded per source table
- [ ] Watermark pattern applied: `WHERE updated_at > COALESCE((SELECT MAX(loaded_watermark) FROM control_table WHERE table_name = 'X'), '1900-01-01')`
- [ ] Since source CSVs are static, simulate incremental behavior via date-based batch loads (e.g., load pre-March 2023 first, then run again to pick up the rest) to prove the mechanism works
- [ ] Document which tables use incremental logic vs. full-refresh, and why (small dimensions vs. large fact tables)

## 5. Idempotency
- [ ] Use `INSERT ... ON CONFLICT (id) DO UPDATE` (upsert) instead of plain `INSERT` for raw/staging/curated loads
- [ ] Every table has a natural or surrogate key that `ON CONFLICT` can target
- [ ] Full-refresh tables use `TRUNCATE` + reload inside a single transaction (not `DELETE` + `INSERT` as separate steps)
- [ ] Explicitly tested: run the DAG twice in a row, confirm row counts are unchanged on the second run

## 6. Modularity
- [ ] Separate scripts/functions per concern: `extract.py`, `clean.py`, `load.py`
- [ ] One function per table/entity rather than one generic loop for all files (different tables need different cleaning logic)
- [ ] Airflow DAG tasks map 1:1 to these modules — no inline SQL/Python scattered across the DAG file
- [ ] Config (file paths, schema names, connection strings) centralized in one place (`.env` / `config.py`)

## 7. Logging & Error Handling
- [ ] Every script/task logs rows read, rows written, and errors — structured logging, not just `print()`
- [ ] Ingestion functions wrapped in try/except with meaningful failure messages, not raw tracebacks
- [ ] At least one cleaning function has a small test/assertion proving correct behavior on a sample input

## 8. Query & Performance Awareness
- [ ] At least one query analyzed with `EXPLAIN ANALYZE`, showing before/after with an index added
- [ ] At least one index created deliberately, with reasoning for the chosen column
- [ ] At least one view created in the curated layer (e.g., `vw_cost_by_diagnosis`) backing the dashboard

## 9. Orchestration (Airflow)
- [ ] Full pipeline (ingest → clean → model) runs as a single DAG
- [ ] Explicit task dependencies, not everything in one task
- [ ] Retry policy configured on at least one task
- [ ] DAG re-runnable without duplicating or corrupting data (ties to idempotency above)

## 10. Data Quality Testing
- [ ] Not-null checks on primary/foreign keys
- [ ] Row count reconciliation: raw vs. curated, with documented reasons for any drop
- [ ] At least one business-rule check (e.g., costs non-negative, dates within reasonable ranges)
- [ ] Quality checks run as part of the DAG with visible pass/fail

## 11. Version Control
- [ ] Meaningful commit history (not one giant commit)
- [ ] At least one feature branch + PR workflow demonstrated
- [ ] `.gitignore` excludes `.env`, raw data dumps, and other files that shouldn't be committed

## 12. Analytics Output (Power BI)
- [ ] Dashboard answers all 4 target questions: cost by diagnosis, top procedures by expense, patient volume over time, provider case load
- [ ] At least one DAX measure (not just drag-and-drop visuals)
- [ ] Dashboard connects live to the curated Postgres schema, not a static export

## 13. Documentation
- [ ] README with setup instructions someone else could actually follow
- [ ] Data dictionary — what each key column and code means
- [ ] Architecture diagram (source → raw → staging → curated → dashboard)
- [ ] Explicit "out of scope / future work" section (e.g., CMS external claims comparison, full incremental coverage, cloud deployment)

---

## Notes
- This checklist is deliberately built so nearly every item maps to something already demonstrated once during the bootcamp (Weeks 2–4: schema normalization, Git workflow, indexing, transactional loaders, watermark incremental loads). The goal is applying proven fundamentals to a harder, real-integration dataset — not learning new concepts under deadline pressure.
- Not every box needs to be checked to demo successfully. Prioritize: sections 1–3, 5–6, 9 (core pipeline) as non-negotiable; sections 4, 8, 10 as strong differentiators; section 13 as what ties the story together for recruiters.