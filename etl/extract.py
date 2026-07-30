import logging 
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

def extract(conn,sql,params=None):
  try:
    df = pd.read_sql_query(sql,conn,params=params)
    logger.info(f"Extracted {len(df)} rows from table")
    return df
  except Exception as e:
    logger.error(str(e))
    raise

def extract_patients(conn):
  sql = """
        SELECT id,birthdate  FROM staging.patients;
    """
  return extract(conn,sql)


def extract_providers(conn):
  sql = """
    SELECT id,organization,name,speciality FROM staging.providers ;
    """
  return extract(conn,sql)

def extract_organizations(conn):
  sql= """
    SELECT id,name,city,state FROM staging.organizations ;
  """
  return extract(conn,sql)  

def extract_encounters(conn,start_date,end_date):
  sql = """
SELECT
	id,
	start_ts,
	stop_ts,
	patient,
	organization,
	provider,
	encounterclass,
	code,
	description,
	total_claim_cost
FROM
	staging.encounters
WHERE start_ts >=%(start_date)s  AND start_ts < %(end_date)s
ORDER BY start_ts;
"""
  return extract(conn,sql,{"start_date":start_date,"end_date":end_date})

def extract_encounters_full(conn):
  return extract_encounters(conn,"2000-01-01","2021-01-01")

def get_watermark(conn) -> datetime:
  """
  Return the most recent start_ts already loaded in the warehouse.
  Falls back to 2000-01-01 on an empty fact table so the first run behaves as a full load without special-casing.
  """
  sql="""
SELECT
	COALESCE(max(start_ts), '2000-01-01'::timestamp) AS watermark
FROM
	fact_encounters;
"""
  df=pd.read_sql_query(sql,conn)
  watermark = df['watermark'].iloc[0]
  logger.info(f"Watermark : {watermark}")
  return watermark


def extract_encounters_incremental(oltp_conn,dw_conn):
  watermark = get_watermark(dw_conn)
  sql = """
SELECT
	id,
	start_ts,
	stop_ts,
	patient,
	organization,
	provider,
	encounterclass,
	code,
	description,
	total_claim_cost
FROM
	staging.encounters
WHERE start_ts > %(watermark)s
ORDER BY start_ts;
"""
  return extract(oltp_conn,sql,{"watermark":watermark})

def extract_procedures(conn,start_date,end_date):
  sql = """
  SELECT
	p.patient,
  p.encounter,
	p.code,
	p.description,
	p.base_cost
FROM
	staging."procedures" p
JOIN staging.encounters e 
ON
	p.encounter = e.id
WHERE
	e.start_ts >= %(start_date)s
	AND e.start_ts < %(end_date)s;
"""
  return extract(conn, sql , {
    "start_date":start_date,
    "end_date":end_date
  })

def extract_procedures_full(conn):
  return extract_procedures(conn,'2000-01-01','2021-01-01')


def extract_procedures_incremental(oltp_conn, dw_conn):
  watermark =get_watermark(dw_conn)
  sql = """
  SELECT
    p.patient,
    p.encounter,
    p.code,
    p.description,
    p.base_cost
  FROM
    staging."procedures" p
  JOIN staging.encounters e 
  ON
    p.encounter = e.id
  WHERE
    e.start_ts > %(watermark)s
  ORDER BY e.start_ts;
"""
  return extract(oltp_conn,sql,{"watermark":watermark})


def extract_conditions(conn,start_date,end_date):
  sql = """
SELECT
	c.patient,
	c.encounter,
	c.code,
	c.description
FROM
	staging.conditions c
JOIN staging.encounters e 
ON
	c.encounter = e.id
WHERE
	e.start_ts >= %(start_date)s
	AND e.start_ts < %(end_date)s;
"""
  return extract(conn,sql,{"start_date":start_date,"end_date":end_date})

def extract_conditions_full(conn):
  return extract_conditions(conn,'2000-01-01','2021-01-01')

def extract_conditions_incremental(oltp_conn,dw_conn):
  watermark = get_watermark(dw_conn)
  sql = """
  SELECT
	c.patient,
	c.encounter,
	c.code,
	c.description
FROM
	staging.conditions c
JOIN staging.encounters e 
ON
	c.encounter = e.id
WHERE
	e.start_ts > %(watermark)s
ORDER BY e.start_ts;
"""
  return extract(oltp_conn,sql,{"watermark":watermark})


def extract_lookup_dim(conn):
  logger.info(f"Loading lookup tables in memory")
  lookup = {
    "patient":pd.read_sql_query("SELECT source_patient_id,patient_key FROM dim_patient",conn),
    "provider":pd.read_sql_query("SELECT source_provider_id, provider_key FROM dim_provider",conn),
    "organization":pd.read_sql_query("SELECT source_organization_id,organization_key FROM dim_organization",conn),
    "date":pd.read_sql_query("SELECT date_key from dim_date",conn),
    "diagnosis":pd.read_sql_query("SELECT code, diagnosis_key FROM dim_diagnosis",conn),
    "procedure":pd.read_sql_query("SELECT code, procedure_key FROM dim_procedure",conn)
  }
  return lookup