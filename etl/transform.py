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