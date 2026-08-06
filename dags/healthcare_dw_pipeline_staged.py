"""
healthcare_db_to_dw_staged.py — staging-table variant of healthcare_dw_pipeline.

Only the 5 dimensions are changed here. Each gets its own
extract_<dim>_to_staging / load_<dim>_from_staging task pair, wired through
a Postgres staging table (etl/staging.py) instead of one monolithic
load_dims task. Benefits over the single-task dim load:
  - dims run in parallel (5 independent branches into fact), visible in the
    Graph view instead of one box.
  - A load failure for one dim doesn't re-hit OLTP for that dim on retry,
    and doesn't block the other 4 dims.
  - The UI shows exactly which dim/stage failed.

fact_encounters is now ALSO split into 4 staged tasks (extract, transform,
quality, load) instead of one monolithic task — same reasoning as the dims:
per-stage retries, and the UI shows exactly which stage failed instead of
one red "load_fact" box. The quality gate is its own task using
AirflowFailException on a genuine DataQualityError, so a real bad-data
failure fails fast instead of burning the 2-retry budget on something a
retry can't fix.

The watermark now originates in extract_fact_to_staging (captured before
any writes happen in this run — same as before) and travels via XCom to
BOTH transform_fact_staged (implicitly, via param) and load_bridges
(explicitly passed in), instead of originating in a single load_fact task.
This is a longer XCom hop than before but not a riskier one: it's still
just one small dict crossing task boundaries, captured before the run's
first write, exactly the property that fixed the original race condition.

bridge_encounter_diagnosis / bridge_encounter_procedure are UNCHANGED —
still one task (load_bridges), tested through full / incremental /
idempotent runs. Not staged further; the ROI isn't there this close to
the demo (see the reasoning in chat — same call made for fact until now).

Staging rows are scoped by batch_id = the Airflow run_id, and cleaned up by
cleanup_staging (trigger_rule="all_done") regardless of whether the run
succeeded or failed.
"""

from datetime import datetime, timedelta

import logging
import time

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, task, Param, get_current_context
from airflow.sdk.exceptions import AirflowFailException

from etl.extract import (
    extract_patients, extract_providers, extract_organizations,
    extract_encounters_full, extract_encounters_incremental,
    extract_procedures_full, extract_procedures_incremental,
    extract_conditions_full, extract_conditions_incremental,
    extract_lookup_dim, extract_encounter_lookup,
    extract_conditions_all, extract_procedures_all, get_watermark,
)
from etl.transform import (
    build_dim_patient, build_dim_provider, build_dim_organization,
    build_dim_diagnosis, build_dim_procedure,
    build_fact_encounters, FACT_COLUMNS,
    build_bridge_encounter_diagnosis, build_bridge_encounter_procedure,
)
from etl.load import (
    load_dim_patient, load_dim_provider, load_dim_organization,
    load_dim_diagnosis, load_dim_procedure,
    load_fact_encounters,
    load_bridge_encounter_diagnosis, load_bridge_encounter_procedure,
)
from etl.quality import run_quality_checks, DataQualityError
from etl.staging import (
    STAGING_TABLES_SQL,
    stage_dim_rows,
    read_dim_staged_rows,
    cleanup_staging_rows,
)

logger = logging.getLogger(__name__)

SRC_CONN_ID = "healthcare_oltp"
DEST_CONN_ID = "healthcare_dw"

# Each dim wired identically (extract+build -> stage -> load), so tasks are
# generated from this config instead of hand-copied 5 times. Each still gets
# its own distinct, independently retryable task_id in the UI.
DIM_CONFIGS = [
    {
        "key": "patient",
        "staging_table": "stg_dim_patient",
        "columns": ["source_patient_id", "birthdate"],
        "extract_fn": extract_patients,
        "build_fn": build_dim_patient,
        "load_fn": load_dim_patient,
    },
    {
        "key": "organization",
        "staging_table": "stg_dim_organization",
        "columns": ["source_organization_id", "name", "city", "state"],
        "extract_fn": extract_organizations,
        "build_fn": build_dim_organization,
        "load_fn": load_dim_organization,
    },
    {
        "key": "provider",
        "staging_table": "stg_dim_provider",
        "columns": ["source_provider_id", "name", "speciality"],
        "extract_fn": extract_providers,
        "build_fn": build_dim_provider,
        "load_fn": load_dim_provider,
    },
    {
        # dims are reference data — use full code history (extract_*_all,
        # no date bound) so newly-appearing codes are available before
        # the bridge loads, same reasoning as the original DAG.
        "key": "diagnosis",
        "staging_table": "stg_dim_diagnosis",
        "columns": ["code", "description"],
        "extract_fn": extract_conditions_all,
        "build_fn": build_dim_diagnosis,
        "load_fn": load_dim_diagnosis,
    },
    {
        "key": "procedure",
        "staging_table": "stg_dim_procedure",
        "columns": ["code", "description"],
        "extract_fn": extract_procedures_all,
        "build_fn": build_dim_procedure,
        "load_fn": load_dim_procedure,
    },
]

# Raw encounter columns as returned by extract_encounters_full/incremental —
# must match stg_fact_extract's DDL in etl/staging.py.
FACT_EXTRACT_COLUMNS = [
    "id", "start_ts", "stop_ts", "patient", "organization", "provider",
    "encounterclass", "code", "description", "total_claim_cost",
]

# Transformed encounter columns — reuse transform.py's own FACT_COLUMNS
# constant so this can't drift out of sync with build_fact_encounters.
FACT_TRANSFORMED_COLUMNS = FACT_COLUMNS


@dag(
    dag_id="healthcare_dw_pipeline_staged",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["healthcare", "dw", "staging-pattern"],
    params={
        "full_reload": Param(
            False,
            type="boolean",
            description="Run full historical load instead of incremental"
        )
    },
)
def healthcare_dw_pipeline_staged():

    create_staging_tables = SQLExecuteQueryOperator(
        task_id="create_staging_tables",
        conn_id=DEST_CONN_ID,
        sql=STAGING_TABLES_SQL,
    )

    def make_dim_staging_tasks(cfg):
        """Build the extract-to-staging / load-from-staging pair for one dim.
        A function call (not a bare loop body) so each pair closes over its
        own cfg — avoids the classic late-binding closure bug with loop vars
        in nested functions."""
        staging_table = cfg["staging_table"]
        columns = cfg["columns"]
        extract_fn = cfg["extract_fn"]
        build_fn = cfg["build_fn"]
        load_fn = cfg["load_fn"]

        @task(task_id=f"extract_{cfg['key']}_to_staging")
        def extract_dim_to_staging(**context):
            batch_id = context["run_id"]
            src_conn = PostgresHook(postgres_conn_id=SRC_CONN_ID).get_conn()
            dst_conn = PostgresHook(postgres_conn_id=DEST_CONN_ID).get_conn()
            try:
                t0 = time.time()
                raw_df = extract_fn(src_conn)
                dim_df = build_fn(raw_df)
                stage_dim_rows(dst_conn, staging_table, columns, dim_df, batch_id)
                logger.info(f"{cfg['key']} staged in {time.time() - t0:.2f}s")
            finally:
                src_conn.close()
                dst_conn.close()

        @task(task_id=f"load_{cfg['key']}_from_staging")
        def load_dim_from_staging(**context):
            batch_id = context["run_id"]
            dst_conn = PostgresHook(postgres_conn_id=DEST_CONN_ID).get_conn()
            try:
                t0 = time.time()
                df = read_dim_staged_rows(dst_conn, staging_table, columns, batch_id)
                load_fn(dst_conn, df)
                logger.info(f"{cfg['key']} loaded from staging in {time.time() - t0:.2f}s")
            finally:
                dst_conn.close()

        extract_task = extract_dim_to_staging()
        load_task = load_dim_from_staging()
        create_staging_tables >> extract_task >> load_task
        return load_task

    dim_load_tasks = [make_dim_staging_tasks(cfg) for cfg in DIM_CONFIGS]

    # --- fact: 4 staged tasks (extract / transform / quality / load) ---

    @task
    def extract_fact_to_staging(**context):
        """Captures the watermark BEFORE any writes happen this run (only in
        the incremental branch), extracts raw encounters, stages them, and
        returns {watermark, full_reload} as a small XCom — consumed by both
        transform_fact_staged (implicitly) and load_bridges (explicitly)."""
        batch_id = context["run_id"]
        full_reload = context["params"]["full_reload"]
        mode = "FULL" if full_reload else "INCREMENTAL"
        logger.info(f"Extracting fact in {mode} mode")

        oltp_conn = PostgresHook(postgres_conn_id=SRC_CONN_ID).get_conn()
        dw_conn = PostgresHook(postgres_conn_id=DEST_CONN_ID).get_conn()
        try:
            t0 = time.time()
            watermark = None
            if full_reload:
                df = extract_encounters_full(oltp_conn)
            else:
                watermark = get_watermark(dw_conn)
                df = extract_encounters_incremental(oltp_conn, watermark)

            stage_dim_rows(dw_conn, "stg_fact_extract", FACT_EXTRACT_COLUMNS, df, batch_id)
            logger.info(f"Fact extract staged in {time.time() - t0:.2f}s")

            return {
                "watermark": str(watermark) if watermark else None,
                "full_reload": full_reload,
            }
        except Exception:
            logger.exception("Fact extract failed")
            raise
        finally:
            oltp_conn.close()
            dw_conn.close()

    @task
    def transform_fact_staged(extract_context, **context):
        batch_id = context["run_id"]
        dw_conn = PostgresHook(postgres_conn_id=DEST_CONN_ID).get_conn()
        try:
            t0 = time.time()
            raw_df = read_dim_staged_rows(dw_conn, "stg_fact_extract", FACT_EXTRACT_COLUMNS, batch_id)
            lookups = extract_lookup_dim(dw_conn)
            fact_df = build_fact_encounters(raw_df, lookups)
            stage_dim_rows(dw_conn, "stg_fact_transformed", FACT_TRANSFORMED_COLUMNS, fact_df, batch_id)
            logger.info(f"Fact transform staged in {time.time() - t0:.2f}s")
        except Exception:
            logger.exception("Fact transform failed")
            raise
        finally:
            dw_conn.close()

    @task
    def quality_check_fact_staged(**context):
        batch_id = context["run_id"]
        dw_conn = PostgresHook(postgres_conn_id=DEST_CONN_ID).get_conn()
        try:
            fact_df = read_dim_staged_rows(dw_conn, "stg_fact_transformed", FACT_TRANSFORMED_COLUMNS, batch_id)
        finally:
            dw_conn.close()

        try:
            run_quality_checks(
                fact_df, 'fact_encounters',
                ['patient_key', 'provider_key', 'organization_key'],
                ['total_claim_cost']
            )
        except DataQualityError as e:
            # Bad data won't pass on retry — fail fast instead of burning
            # the DAG's retry budget on a non-transient error.
            raise AirflowFailException(str(e)) from e

    @task
    def load_fact_from_staging(**context):
        batch_id = context["run_id"]
        dw_conn = PostgresHook(postgres_conn_id=DEST_CONN_ID).get_conn()
        try:
            t0 = time.time()
            fact_df = read_dim_staged_rows(dw_conn, "stg_fact_transformed", FACT_TRANSFORMED_COLUMNS, batch_id)
            load_fact_encounters(dw_conn, fact_df)
            logger.info(f"Fact loaded from staging in {time.time() - t0:.2f}s")
        except Exception:
            logger.exception("Fact load failed")
            raise
        finally:
            dw_conn.close()

    @task
    def load_bridges(load_context):
        oltp_conn = PostgresHook(postgres_conn_id=SRC_CONN_ID).get_conn()
        dw_conn = PostgresHook(postgres_conn_id=DEST_CONN_ID).get_conn()

        watermark = (
            datetime.fromisoformat(load_context["watermark"])
            if load_context["watermark"]
            else None
        )
        full_reload = load_context["full_reload"]
        t0 = time.time()

        try:
            lookups = extract_lookup_dim(dw_conn)
            lookups["encounter"] = extract_encounter_lookup(dw_conn)

            if full_reload:
                batch_conditions_df = extract_conditions_full(oltp_conn)
                batch_procedures_df = extract_procedures_full(oltp_conn)
            else:
                batch_conditions_df = extract_conditions_incremental(oltp_conn, watermark)
                batch_procedures_df = extract_procedures_incremental(oltp_conn, watermark)

            bridge_encounter_diagnosis_df = build_bridge_encounter_diagnosis(batch_conditions_df, lookups)
            run_quality_checks(bridge_encounter_diagnosis_df, 'bridge_encounter_diagnosis', ['encounter_key', 'diagnosis_key'])
            load_bridge_encounter_diagnosis(dw_conn, bridge_encounter_diagnosis_df)

            bridge_encounter_procedure_df = build_bridge_encounter_procedure(batch_procedures_df, lookups)
            run_quality_checks(bridge_encounter_procedure_df, 'bridge_encounter_procedure', ['encounter_key', 'procedure_key'], ['base_cost'])
            load_bridge_encounter_procedure(dw_conn, bridge_encounter_procedure_df)

            logger.info(f"Bridge load completed in {time.time() - t0:.2f}s")
        except Exception:
            logger.exception("Bridge load failed")
            raise
        finally:
            oltp_conn.close()
            dw_conn.close()

    @task(trigger_rule="all_done")
    def cleanup_staging(**context):
        batch_id = context["run_id"]
        dst_conn = PostgresHook(postgres_conn_id=DEST_CONN_ID).get_conn()
        try:
            cleanup_staging_rows(dst_conn, batch_id)
        finally:
            dst_conn.close()

    extract_fact = extract_fact_to_staging()
    create_staging_tables >> extract_fact  # same guard the dim extracts already have — was missing here

    transform_fact = transform_fact_staged(extract_fact)
    quality_fact = quality_check_fact_staged()
    load_fact_task = load_fact_from_staging()

    # transform_fact needs dim keys already loaded (extract_lookup_dim reads
    # dim_patient/provider/organization), so it waits on dims explicitly —
    # separate from its implicit dependency on extract_fact via the XCom param.
    dim_load_tasks >> transform_fact
    transform_fact >> quality_fact >> load_fact_task

    # load_bridges gets its watermark from extract_fact_to_staging (captured
    # before this run's first write) — same property that fixed the original
    # race condition, just crossing one more task boundary now.
    bridges = load_bridges(extract_fact)
    load_fact_task >> bridges  # bridges reads fact_encounters via extract_encounter_lookup, so it must wait for the actual load, not just extract

    bridges >> cleanup_staging()


healthcare_dw_pipeline_staged()