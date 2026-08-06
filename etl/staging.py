"""
staging.py
----------
Helpers for the staging-table pattern used by the 5 dimension tasks in
dags/healthcare_db_to_dw_staged.py. Each dim gets its own staging table
in healthcare_dw, scoped per-run by batch_id (the Airflow run_id), so:
  - extract_<dim>_to_staging and load_<dim>_from_staging are independently
    retryable without re-hitting the OLTP source on a load-only failure.
  - No row-level data crosses task boundaries via XCom.

fact_encounters and the two bridge tables are NOT staged here — they stay
on the tested extract/transform/load-in-one-task pattern from the original
DAG, since that path is verified correct (watermark XCom fix) and isn't
worth the added surface area this close to the demo.
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)

STAGING_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS stg_dim_patient (
    batch_id TEXT NOT NULL,
    source_patient_id TEXT,
    birthdate DATE
);
CREATE INDEX IF NOT EXISTS idx_stg_dim_patient_batch ON stg_dim_patient(batch_id);

CREATE TABLE IF NOT EXISTS stg_dim_provider (
    batch_id TEXT NOT NULL,
    source_provider_id TEXT,
    name TEXT,
    speciality TEXT
);
CREATE INDEX IF NOT EXISTS idx_stg_dim_provider_batch ON stg_dim_provider(batch_id);

CREATE TABLE IF NOT EXISTS stg_dim_organization (
    batch_id TEXT NOT NULL,
    source_organization_id TEXT,
    name TEXT,
    city TEXT,
    state TEXT
);
CREATE INDEX IF NOT EXISTS idx_stg_dim_organization_batch ON stg_dim_organization(batch_id);

CREATE TABLE IF NOT EXISTS stg_dim_diagnosis (
    batch_id TEXT NOT NULL,
    code TEXT,
    description TEXT
);
CREATE INDEX IF NOT EXISTS idx_stg_dim_diagnosis_batch ON stg_dim_diagnosis(batch_id);

CREATE TABLE IF NOT EXISTS stg_dim_procedure (
    batch_id TEXT NOT NULL,
    code TEXT,
    description TEXT
);
CREATE INDEX IF NOT EXISTS idx_stg_dim_procedure_batch ON stg_dim_procedure(batch_id);

-- Raw encounters as extracted from OLTP (either full or incremental range),
-- before any dim-key lookups have been applied.
CREATE TABLE IF NOT EXISTS stg_fact_extract (
    batch_id TEXT NOT NULL,
    id TEXT,
    start_ts TIMESTAMP,
    stop_ts TIMESTAMP,
    patient TEXT,
    organization TEXT,
    provider TEXT,
    encounterclass TEXT,
    code TEXT,
    description TEXT,
    total_claim_cost NUMERIC
);
CREATE INDEX IF NOT EXISTS idx_stg_fact_extract_batch ON stg_fact_extract(batch_id);

-- Encounters after build_fact_encounters — dim keys resolved, ready for
-- the quality gate and then load_fact_encounters. Column set mirrors
-- transform.py's FACT_COLUMNS.
CREATE TABLE IF NOT EXISTS stg_fact_transformed (
    batch_id TEXT NOT NULL,
    source_encounter_id TEXT,
    patient_key INTEGER,
    provider_key INTEGER,
    organization_key INTEGER,
    date_key DATE,
    encounterclass TEXT,
    code TEXT,
    description TEXT,
    total_claim_cost NUMERIC,
    duration_minutes NUMERIC,
    start_ts TIMESTAMP,
    stop_ts TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_stg_fact_transformed_batch ON stg_fact_transformed(batch_id);
"""

# Every staging table this DAG owns — used by cleanup_staging_rows.
ALL_STAGING_TABLES = [
    "stg_dim_patient",
    "stg_dim_provider",
    "stg_dim_organization",
    "stg_dim_diagnosis",
    "stg_dim_procedure",
    "stg_fact_extract",
    "stg_fact_transformed",
]


def _records(df: pd.DataFrame, columns: list) -> list:
    """DataFrame -> list[dict] restricted to `columns`, NaN/NaT -> None."""
    sub = df[columns]
    return sub.astype(object).where(pd.notnull(sub), None).to_dict("records")


def stage_dim_rows(dst_conn, staging_table: str, columns: list, df: pd.DataFrame, batch_id: str):
    """
    Generic — despite the name, this works for ANY staging table (dims,
    stg_fact_extract, stg_fact_transformed), since it only ever operates on
    the staging_table/columns passed in. Kept the "dim" name since that's
    where the pattern started; not worth a rename mid-project.

    Write df into staging_table under batch_id.
    Deletes any existing rows for this batch_id first, so a task RETRY
    (same run_id, same batch_id) doesn't duplicate previously-staged rows.
    """
    col_list = ", ".join(columns)
    placeholders = ", ".join(f"%({c})s" for c in columns)
    del_sql = f"DELETE FROM {staging_table} WHERE batch_id = %(batch_id)s"
    ins_sql = f"INSERT INTO {staging_table} (batch_id, {col_list}) VALUES (%(batch_id)s, {placeholders})"

    try:
        with dst_conn.cursor() as cur:
            cur.execute(del_sql, {"batch_id": batch_id})

            if df.empty:
                logger.info(f"No rows to stage for {staging_table} (batch {batch_id})")
                dst_conn.commit()
                return

            records = _records(df, columns)
            for r in records:
                r["batch_id"] = batch_id
            cur.executemany(ins_sql, records)
            logger.info(f"{cur.rowcount} rows staged to {staging_table} (batch {batch_id})")
        dst_conn.commit()
    except Exception as e:
        dst_conn.rollback()
        logger.error(str(e))
        raise


def read_dim_staged_rows(dst_conn, staging_table: str, columns: list, batch_id: str) -> pd.DataFrame:
    """Read this batch's staged rows back out as a DataFrame with exactly `columns`."""
    col_list = ", ".join(columns)
    sql = f"SELECT {col_list} FROM {staging_table} WHERE batch_id = %(batch_id)s"
    df = pd.read_sql_query(sql, dst_conn, params={"batch_id": batch_id})
    logger.info(f"Read {len(df)} staged rows from {staging_table} (batch {batch_id})")
    return df


def cleanup_staging_rows(dst_conn, batch_id: str, staging_tables: list = None):
    """Delete this run's rows from every staging table. Call with trigger_rule='all_done'
    so it runs even if an earlier task in the run failed."""
    tables = staging_tables or ALL_STAGING_TABLES
    try:
        with dst_conn.cursor() as cur:
            for t in tables:
                cur.execute(f"DELETE FROM {t} WHERE batch_id = %(batch_id)s", {"batch_id": batch_id})
        dst_conn.commit()
        logger.info(f"Cleaned staging rows for batch {batch_id} across {len(tables)} tables")
    except Exception as e:
        dst_conn.rollback()
        logger.error(str(e))
        raise