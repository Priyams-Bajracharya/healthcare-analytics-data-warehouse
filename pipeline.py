import argparse
import logging
import os
import time

import psycopg2
from dotenv import load_dotenv

from etl.extract import (
  extract_patients, extract_providers, extract_organizations,
  extract_encounters_full, extract_encounters_incremental,
  extract_procedures_full, extract_procedures_incremental,
  extract_conditions_full, extract_conditions_incremental,
  extract_lookup_dim, extract_encounter_lookup, get_watermark,
  extract_conditions_all,extract_procedures_all,
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

def parse_args():
  parser = argparse.ArgumentParser(description="Healthcare DW ETL pipeline")
  parser.add_argument(
    "--full-reload",
    action="store_true",
    help="Full load (2000-2020) instead of incremental (default: incremental)"
  )
  return parser.parse_args()

logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s  %(levelname)s [%(filename)s:%(lineno)d] %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()

OLTP_DB_CONFIG = dict(
  host=os.getenv("OLTP_DB_HOST"),
  port=os.getenv("OLTP_DB_PORT"),
  dbname=os.getenv("OLTP_DB_NAME"),
  user=os.getenv("OLTP_DB_USER"),
  password=os.getenv("OLTP_DB_PASSWORD"),
)
DW_DB_CONFIG = dict(
  host=os.getenv("DW_DB_HOST"),
  port=os.getenv("DW_DB_PORT"),
  dbname=os.getenv("DW_DB_NAME"),
  user=os.getenv("DW_DB_USER"),
  password=os.getenv("DW_DB_PASSWORD"),
)

def main():
  args = parse_args()
  mode = 'FULL' if args.full_reload else 'INCREMENTAL'

  oltp_conn = psycopg2.connect(**OLTP_DB_CONFIG)
  dw_conn = psycopg2.connect(**DW_DB_CONFIG)
  try:
    # --- Stage 1: dims ---
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

    diagnosis_df = extract_conditions_all(oltp_conn)
    diagnosis_dim = build_dim_diagnosis(diagnosis_df)
    load_dim_diagnosis(dw_conn,diagnosis_dim)

    procedure_df =extract_procedures_all(oltp_conn)
    procedure_dim = build_dim_procedure(procedure_df)
    load_dim_procedure(dw_conn,procedure_dim)

    logger.info(f"Dim load completed in {time.time() - t0:.2f}s")

    # --- Stage 2: fact ---
    t0 = time.time()
    lookups = extract_lookup_dim(dw_conn)

    if mode == 'INCREMENTAL':
      batch_encounters_df = extract_encounters_incremental(oltp_conn, dw_conn)
      batch_procedures_df = extract_procedures_incremental(oltp_conn, dw_conn)
      batch_conditions_df = extract_conditions_incremental(oltp_conn, dw_conn)
    else:
      batch_encounters_df = extract_encounters_full(oltp_conn)
      batch_procedures_df = extract_procedures_full(oltp_conn)
      batch_conditions_df = extract_conditions_full(oltp_conn)

    fact_encounters = build_fact_encounters(batch_encounters_df,lookups)
    run_quality_checks(fact_encounters ,'fact_encounters',['patient_key','provider_key','organization_key'],['total_claim_cost'])
    load_fact_encounters(dw_conn,fact_encounters)
    logger.info(f"Fact load completed in {time.time() - t0:.2f}s")

    # --- Stage 3: bridges ---
    t0 = time.time()
    lookups["encounter"] = extract_encounter_lookup(dw_conn)
    bridge_encounter_diagnosis_df = build_bridge_encounter_diagnosis(batch_conditions_df,lookups)
    run_quality_checks(bridge_encounter_diagnosis_df,'bridge_encounter_diagnosis',['encounter_key','diagnosis_key'])
    load_bridge_encounter_diagnosis(dw_conn,bridge_encounter_diagnosis_df)

    bridge_encounter_procedure_df = build_bridge_encounter_procedure(batch_procedures_df,lookups)
    run_quality_checks(bridge_encounter_procedure_df,'bridge_encounter_procedure',['encounter_key','procedure_key'],['base_cost'])
    load_bridge_encounter_procedure(dw_conn,bridge_encounter_procedure_df)
    logger.info(f"Bridge load completed in {time.time() - t0:.2f}s")

  finally:
      oltp_conn.close()
      dw_conn.close()

if __name__ == "__main__":
    main()