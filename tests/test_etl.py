import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
import psycopg2

from etl.extract import extract_patients,extract_providers,extract_organizations,extract_encounters_full,extract_conditions_full,extract_procedures_full

from etl.transform import build_dim_patient, build_dim_provider, build_dim_organization
from etl.transform import build_dim_diagnosis, build_dim_procedure

from etl.load import load_dim_patient, load_dim_provider, load_dim_organization, load_dim_diagnosis, load_dim_procedure

from etl.extract import extract_lookup_dim
from etl.transform import build_fact_encounters

from etl.load import load_fact_encounters

from etl.extract import extract_encounter_lookup
from etl.transform import build_bridge_encounter_diagnosis, build_bridge_encounter_procedure

from etl.load import load_bridge_encounter_diagnosis, load_bridge_encounter_procedure






load_dotenv()

def get_oltp_connection():
  return psycopg2.connect(
    host=os.getenv("OLTP_DB_HOST"),
    port=os.getenv("OLTP_DB_PORT"),
    dbname=os.getenv("OLTP_DB_NAME"),
    user=os.getenv("OLTP_DB_USER"),
    password=os.getenv("OLTP_DB_PASSWORD"),
  )

def get_dw_connection():
  # same pattern, DW_ prefixed vars — fill this in yourself
  return psycopg2.connect(
   host = os.getenv("DW_DB_HOST"),
   port = os.getenv("DW_DB_PORT"),
   dbname = os.getenv("DW_DB_NAME"),
   user= os.getenv("DW_DB_USER"),
   password = os.getenv("DW_DB_PASSWORD"),  
  )

if __name__ == "__main__":
    conn = get_oltp_connection()
    dw_conn = get_dw_connection()

    df_patients = extract_patients(conn)
    df_providers =extract_providers(conn)
    df_organizations = extract_organizations(conn)
    df_encounters = extract_encounters_full(conn)
    df_procedures = extract_procedures_full(conn)
    df_conditions = extract_conditions_full(conn)

    print("Patients:", df_patients.shape)
    print("Providers:",df_providers.shape)
    print("Organizations:",df_organizations.shape)
    print("Encounters:",df_encounters.shape)
    print(df_encounters.dtypes)
    print("Procedures:",df_procedures.shape)
    print("Conditions:",df_conditions.shape)


    dim_patient = build_dim_patient(df_patients)
    print("dim_patient columns:", dim_patient.columns.tolist())
    print(dim_patient.head())

    dim_provider = build_dim_provider(df_providers)
    print("dim_provider columns:", dim_provider.columns.tolist())

    dim_organization = build_dim_organization(df_organizations)
    print("dim_organization columns:", dim_organization.columns.tolist())


    dim_diagnosis = build_dim_diagnosis(df_conditions)
    print("dim_diagnosis shape:", dim_diagnosis.shape)
    print(dim_diagnosis.head())

    dim_procedure = build_dim_procedure(df_procedures)
    print("dim_procedure shape:", dim_procedure.shape)
    print(dim_procedure.head())

        
    load_dim_patient(dw_conn, dim_patient)
    load_dim_provider(dw_conn, dim_provider)
    load_dim_organization(dw_conn, dim_organization)
    load_dim_diagnosis(dw_conn, dim_diagnosis)
    load_dim_procedure(dw_conn, dim_procedure)  


    lookups = extract_lookup_dim(dw_conn)
    for k, v in lookups.items():
        print(k, v.shape)  

    fact_encounters = build_fact_encounters(df_encounters, lookups)
    print("fact_encounters shape:", fact_encounters.shape)
    print(fact_encounters.dtypes)
    print(fact_encounters.head())

        
    load_fact_encounters(dw_conn, fact_encounters) 

        
    encounter_lookup = extract_encounter_lookup(dw_conn)
    print("encounter_lookup shape:", encounter_lookup.shape)

    # build a combined lookups dict for bridges — needs encounter + diagnosis/procedure
    bridge_lookups = dict(lookups)  # reuse the earlier lookups dict
    bridge_lookups["encounter"] = encounter_lookup

    bridge_diagnosis = build_bridge_encounter_diagnosis(df_conditions, bridge_lookups)
    print("bridge_diagnosis shape:", bridge_diagnosis.shape)

    bridge_procedure = build_bridge_encounter_procedure(df_procedures, bridge_lookups)
    print("bridge_procedure shape:", bridge_procedure.shape) 

        
    load_bridge_encounter_diagnosis(dw_conn, bridge_diagnosis)
    load_bridge_encounter_procedure(dw_conn, bridge_procedure)  

    conn.close()