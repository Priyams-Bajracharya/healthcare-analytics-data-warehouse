import logging
import pandas as pd

logger = logging.getLogger(__name__)

def build_dim_patient(patients_df: pd.DataFrame) -> pd.DataFrame:
  df = patients_df.copy()
  df = df.rename(columns= {"id":"source_patient_id"})
  logger.info(f"Built patient dimension with {len(df)} rows")
  return df

def build_dim_provider(provider_df : pd.DataFrame) -> pd.DataFrame:
  df = provider_df.copy()
  df = df.rename(columns= {"id":"source_provider_id"})
  df = df.drop(columns = ['organization'])
  logger.info(f"Built provider dimension with {len(df)} rows")
  return df

def build_dim_organization(organization_df : pd.DataFrame) -> pd.DataFrame:
  df = organization_df.copy()
  df = df.rename(columns= {"id":"source_organization_id"})
  logger.info(f"Built organization dimension with {len(df)} rows")
  return df

def build_dim_diagnosis(conditions_df: pd.DataFrame) -> pd.DataFrame :
  df = conditions_df.copy()
  df = df[['code','description']]
  df =df.drop_duplicates()
  df =df.reset_index(drop=True)
  logger.info(f"Built diagnosis dimension with {len(df)} rows")
  return df

def build_dim_procedure(procedure_df:pd.DataFrame) -> pd.DataFrame :
  df = procedure_df.copy()
  df = df[['code','description']]
  df = df.drop_duplicates()
  df = df.reset_index(drop=True)
  logger.info(f"Built procedures dimension with {len(df)} rows")
  return df

def _drop_unmatched(df: pd.DataFrame, key_col: str, label: str) -> pd.DataFrame:
    missing = df[key_col].isna()
    if missing.any():
        logger.warning(f"{missing.sum()} encounter(s) missing {label} — skipped")
    return df[~missing]

FACT_COLUMNS = [
    "source_encounter_id",
    "patient_key",
    "provider_key",
    "organization_key",
    "date_key",
    "encounterclass",
    "code",
    "description",
    "total_claim_cost",
    "duration_minutes",
    "start_ts",
    "stop_ts",
]

#transforming for fact tables using lookups
def build_fact_encounters(encounters_df:pd.DataFrame , lookups: dict) -> pd.DataFrame:
  if encounters_df.empty:
    logger.info("No encounters extracted - nothing to transform")
    return pd.DataFrame(columns=FACT_COLUMNS)

  initial_count = len(encounters_df)
  df = encounters_df.copy()

  #date_key
  df["date_key"]=df["start_ts"].dt.date
  df = df[df["date_key"].isin(lookups["date"]["date_key"])]
  dropped = initial_count - len(df)
  if dropped > 0:
    logger.warning(f"{dropped} encounter(s) with date outside dim_date range — skipped")

  #merge patient
  df = df.merge(
    lookups["patient"],
    left_on="patient",
    right_on="source_patient_id",
    how="left"
  )
  df = _drop_unmatched(df,'patient_key','patient_key(dim_patient)')

  df = df.merge(
    lookups["provider"],
    left_on="provider",
    right_on="source_provider_id",
    how="left"
  )
  df= _drop_unmatched(df,'provider_key','provider_key(dim_provider)')

  df = df.merge(
    lookups["organization"],
    left_on="organization",
    right_on="source_organization_id",
    how="left"
  )
  df = _drop_unmatched(df, 'organization_key', 'organization_key(dim_organization)')

  df["duration_minutes"] =((df['stop_ts'] - df['start_ts']).dt.total_seconds()/60).round(1)

  df = df.rename(columns = {"id":"source_encounter_id"})

  result = df[FACT_COLUMNS].reset_index(drop=True)
  logger.info(f"Transformed {len(result)} rows, skipped {initial_count - len(result)}")

  return result