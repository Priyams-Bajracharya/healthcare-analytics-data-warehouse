from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task, Param, get_current_context
from datetime import timedelta

import time
import logging

logger = logging.getLogger(__name__)

from etl.extract import (
  extract_patients, extract_providers, extract_organizations,
  extract_encounters_full, extract_encounters_incremental,
  extract_procedures_full, extract_procedures_incremental,
  extract_conditions_full, extract_conditions_incremental,
  extract_lookup_dim, extract_encounter_lookup, 
  extract_conditions_all,extract_procedures_all,get_watermark
)
from etl.transform import (
  build_dim_patient, build_dim_provider, build_dim_organization,
  build_dim_diagnosis, build_dim_procedure,
  build_fact_encounters,
  build_bridge_encounter_diagnosis, build_bridge_encounter_procedure,
)
from etl.load import (
  load_dim_patient, load_dim_provider, load_dim_organization,
  load_dim_diagnosis, load_dim_procedure,
  load_fact_encounters,
  load_bridge_encounter_diagnosis, load_bridge_encounter_procedure,
)

from etl.quality import (
   run_quality_checks
)

SRC_CONN_ID = "healthcare_oltp"
DEST_CONN_ID= "healthcare_dw"

@dag(
  dag_id="healthcare_dw_pipeline",
  schedule=None,          # manual trigger for now — think about whether this should change later
  start_date=datetime(2026, 1, 1),
  catchup=False,
  default_args={
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
  },
  tags=["healthcare", "dw"],
  params = {
    "full_reload":Param(
      False,
      type="boolean",
      description="Run full historical load instead of incremental"
    )
  }
)
def healthcare_dw_pipeline():

  @task
  def load_dims():
    oltp_conn = PostgresHook(postgres_conn_id=SRC_CONN_ID).get_conn()   # <- which conn_id? check your .env AIRFLOW_CONN_ names
    dw_conn = PostgresHook(postgres_conn_id=DEST_CONN_ID).get_conn()

    try:
      t0 = time.time()
      patient_df = extract_patients(oltp_conn)
      patient_dim = build_dim_patient(patient_df)
      load_dim_patient(dw_conn,patient_dim)

      organization_df = extract_organizations(oltp_conn)
      organization_dim  = build_dim_organization(organization_df)
      load_dim_organization(dw_conn, organization_dim)

      provider_df = extract_providers(oltp_conn)
      provider_dim = build_dim_provider(provider_df)
      load_dim_provider(dw_conn,provider_dim)

      # Dimensions are reference data, not event data.
      # Use full code history so newly appearing diagnosis/procedure
      # codes are available before bridge loads.
      diagnosis_df = extract_conditions_all(oltp_conn)
      diagnosis_dim = build_dim_diagnosis(diagnosis_df)
      load_dim_diagnosis(dw_conn,diagnosis_dim)

      procedure_df =extract_procedures_all(oltp_conn)
      procedure_dim = build_dim_procedure(procedure_df)
      load_dim_procedure(dw_conn,procedure_dim)

      logger.info(f"Dim load completed in {time.time() - t0:.2f}s")
    except Exception as e:
      logger.exception("Dimension load failed")
      raise
    finally:
      oltp_conn.close()
      dw_conn.close()

    # call your existing extract/transform/load functions here
    # for all 5 dims, same as pipeline.py's "Stage 1: dims" block


  @task
  def load_fact():
    # same connection pattern
    # same as pipeline.py's "Stage 2: fact" block
    oltp_conn = PostgresHook(postgres_conn_id=SRC_CONN_ID).get_conn()

    dw_conn = PostgresHook(postgres_conn_id =DEST_CONN_ID).get_conn()

    try:
      context = get_current_context()
      full_reload = context["params"]["full_reload"]

      mode = "FULL" if full_reload else "INCREMENTAL"

      logger.info(f"Running fact load in {mode} mode")

      t0 = time.time()
      lookups = extract_lookup_dim(dw_conn)

      watermark = None

      if full_reload:
        batch_encounters_df = extract_encounters_full(oltp_conn)

      else:
        watermark = get_watermark(dw_conn)
        batch_encounters_df = extract_encounters_incremental(oltp_conn, watermark)


      fact_encounters = build_fact_encounters(batch_encounters_df,lookups)
      run_quality_checks(fact_encounters ,'fact_encounters',['patient_key','provider_key','organization_key'],['total_claim_cost'])
      load_fact_encounters(dw_conn,fact_encounters)
      logger.info(f"Fact load completed in {time.time() - t0:.2f}s")

      return {
        "watermark":str(watermark) if watermark else None,
        "full_reload":full_reload
      } #becomes an XCom 
    except Exception as e:
      logger.exception("Fact load failed")
      raise
    finally:
      oltp_conn.close()
      dw_conn.close()
    

  @task
  def load_bridges(load_context):
    # same as pipeline.py's "Stage 3: bridges" block
    oltp_conn = PostgresHook(postgres_conn_id=SRC_CONN_ID).get_conn()
    dw_conn = PostgresHook(postgres_conn_id=DEST_CONN_ID).get_conn()

    watermark = (datetime.fromisoformat(load_context["watermark"])
    if load_context["watermark"]
    else None
    )
    full_reload = load_context["full_reload"]
    t0 = time.time()

    try:
      lookups = extract_lookup_dim(dw_conn)
      lookups["encounter"] =extract_encounter_lookup(dw_conn)


      if full_reload:

          batch_conditions_df = extract_conditions_full(
            oltp_conn
          )

          batch_procedures_df = extract_procedures_full(
            oltp_conn
          )

      else:

          batch_conditions_df = extract_conditions_incremental(
            oltp_conn,
            watermark
          )

          batch_procedures_df = extract_procedures_incremental(
            oltp_conn,
            watermark
          )

      bridge_encounter_diagnosis_df = build_bridge_encounter_diagnosis(batch_conditions_df,lookups)

      run_quality_checks(bridge_encounter_diagnosis_df,'bridge_encounter_diagnosis',['encounter_key','diagnosis_key'])

      load_bridge_encounter_diagnosis(dw_conn,bridge_encounter_diagnosis_df)

      bridge_encounter_procedure_df = build_bridge_encounter_procedure(batch_procedures_df,lookups)

      run_quality_checks(bridge_encounter_procedure_df,'bridge_encounter_procedure',['encounter_key','procedure_key'],['base_cost'])

      load_bridge_encounter_procedure(dw_conn,bridge_encounter_procedure_df)

      logger.info(f"Bridge load completed in {time.time() - t0:.2f}s")
    except Exception as e:
      logger.exception("Bridge Load failed")
      raise
    finally:
      oltp_conn.close()
      dw_conn.close()


  dims = load_dims()
  fact = load_fact()
  bridges = load_bridges(fact) # passing facts return value in directly , wires the xcom
  # wire the ordering here
  dims >> fact >> bridges

healthcare_dw_pipeline()