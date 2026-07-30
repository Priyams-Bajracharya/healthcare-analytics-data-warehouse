import logging

import pandas as pd

logger = logging.getLogger(__name__)


def _records(df: pd.DataFrame) -> list:
  """DataFrame -> list[dict], turning NaN/NaT/pd.NA into None so psycopg2
   writes SQL NULL instead of the literal string 'nan'."""
  return df.astype(object).where(pd.notnull(df), None).to_dict("records")


def _execute(conn, sql, df: pd.DataFrame, table_name: str):
  if df.empty:
      logger.info(f"No rows to load — skipping {table_name}")
      return
  try:
      with conn.cursor() as curr:
          curr.executemany(sql, _records(df))
          logger.info(f"{curr.rowcount} inserted to {table_name}")
      conn.commit()
  except Exception as e:
      conn.rollback()
      logger.error(str(e))
      raise

def load_dim_patient(conn,patients_df):
  sql = """

  INSERT
  INTO
  dim_patient (source_patient_id ,
  birthdate)
  VALUES(%(source_patient_id)s,%(birthdate)s)
  ON CONFLICT (source_patient_id) DO NOTHING;
  """
  _execute(conn,sql,patients_df,'dim_patient')

def load_dim_provider(conn,provider_df):
  sql = """
  INSERT
  INTO
  dim_provider (source_provider_id ,
  name,speciality)
  VALUES(%(source_provider_id)s,%(name)s,%(speciality)s)
  ON CONFLICT (source_provider_id) DO NOTHING;
"""  
  _execute(conn,sql,provider_df,'dim_provider')

def load_dim_organization(conn,organization_df):
  sql = """
  INSERT
  INTO
  dim_organization (source_organization_id ,name,city,state)
  VALUES(%(source_organization_id)s,%(name)s,%(city)s,%(state)s)
  ON CONFLICT (source_organization_id) DO NOTHING;
"""
  _execute(conn,sql,organization_df,'dim_organization')

def load_dim_diagnosis(conn,diagnosis_df):
  sql = """
  INSERT
  INTO
  dim_diagnosis (code ,description)
  VALUES(%(code)s,%(description)s)
  ON CONFLICT (code) DO NOTHING;
"""  
  _execute(conn,sql,diagnosis_df,'dim_diagnosis')


def load_dim_procedure(conn,procedure_df):
  sql = """
  INSERT
  INTO
  dim_procedure (code ,description)
  VALUES(%(code)s,%(description)s)
  ON CONFLICT (code) DO NOTHING;
"""  
  _execute(conn,sql,procedure_df,'dim_procedure')  

def load_fact_encounters(conn, encounters_df):
  sql = """
  INSERT 
  INTO 
  fact_encounters(source_encounter_id, patient_key, provider_key , organization_key , date_key , encounterclass , code , description , total_claim_cost , duration_minutes , start_ts , stop_ts)
  VALUES(%(source_encounter_id)s,%(patient_key)s,%(provider_key)s,%(organization_key)s,%(date_key)s,%(encounterclass)s,%(code)s,%(description)s,%(total_claim_cost)s,%(duration_minutes)s,%(start_ts)s,%(stop_ts)s)
  ON CONFLICT (source_encounter_id) DO NOTHING;
"""
  _execute(conn,sql,encounters_df,'fact_encounters')