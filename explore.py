"""
Exploration script for Synthea synthetic healthcare data.
Each function explores one file or one specific question about the data.
Run functions individually by commenting/uncommenting calls in main().
"""

import pandas as pd
import os

FOLDER = os.path.join('synthea_sample_data_csv_nov2021', 'csv')


def load(filename):
    """Load a CSV from the data folder by name (no .csv needed)."""
    return pd.read_csv(os.path.join(FOLDER, f'{filename}.csv'))


def row_counts(files):
    """Print row/column counts for a list of file names."""
    for f in files:
        df = load(f)
        print(f"{f}: {df.shape[0]} rows, {df.shape[1]} columns")


def explore_patients():
    df = load('patients')
    print("--- patients.csv ---")
    print(df.columns.tolist())
    print(df.head())
    deceased = df['DEATHDATE'].notna().sum()
    print(f"{deceased} patients deceased out of {len(df)}")
    print("Null counts per column:")
    print(df.isna().sum())
    return df


def explore_encounters():
    df = load('encounters')
    print("--- encounters.csv ---")
    print(df.columns.tolist())
    print(df.head())
    print("Date range:", df['START'].min(), "to", df['START'].max())
    print("Null counts per column:")
    print(df.isna().sum())
    df['START'] = pd.to_datetime(df['START'])
    print("\n--- Encounters by decade ---")
    print((df['START'].dt.year // 10 * 10).value_counts().sort_index())

    df['START'] = pd.to_datetime(df['START'])
    year_2021 = df[df['START'].dt.year == 2021]
    print("\n--- 2021 encounters by month ---")
    print(year_2021['START'].dt.month.value_counts().sort_index())
    return df


def explore_conditions():
    df = load('conditions')
    print("--- conditions.csv ---")
    print(df.columns.tolist())
    print(df.head())
    print(f"{df['CODE'].nunique()} unique diagnosis codes")
    print("Null counts per column:")
    print(df.isna().sum())
    return df


def explore_procedures():
    df = load('procedures')
    print("--- procedures.csv ---")
    print(df.columns.tolist())
    print(df.head())
    print(f"{df['CODE'].nunique()} unique procedure codes")
    print("Null counts per column:")
    print(df.isna().sum())
    return df


def explore_providers():
    df = load('providers')
    print("--- providers.csv ---")
    print(df.columns.tolist())
    print(df.head())
    print("Null counts per column:")
    print(df.isna().sum())
    return df


def explore_organizations():
    df = load('organizations')
    print("--- organizations.csv ---")
    print(df.columns.tolist())
    print(df.head())
    print("Null counts per column:")
    print(df.isna().sum())
    return df


def explore_claims():
    df = load('claims')
    print("--- claims.csv ---")
    print(df.columns.tolist())
    print(df.head())
    print("Null counts per column:")
    print(df.isna().sum())
    return df


def explore_claims_transactions():
    df = load('claims_transactions')
    print("--- claims_transactions.csv ---")
    print(df.columns.tolist())
    print(df.head())
    print("Null counts per column:")
    print(df.isna().sum())
    return df


def verify_encounter_claims_join():
    """Confirm claims_transactions.APPOINTMENTID matches encounters.Id."""
    encounters = load('encounters')
    transactions = load('claims_transactions')

    encounter_ids = set(encounters['Id'])
    appointment_ids = set(transactions['APPOINTMENTID'].dropna())
    overlap = encounter_ids.intersection(appointment_ids)

    print("--- Encounter <-> Claims Transactions join check ---")
    print(f"Encounter IDs: {len(encounter_ids)}")
    print(f"Appointment IDs in transactions: {len(appointment_ids)}")
    print(f"Overlap: {len(overlap)}")


def main():
    files = ['patients', 'encounters', 'conditions', 'procedures',
              'claims', 'claims_transactions', 'providers', 'organizations']

    # Uncomment the exploration(s) you want to run:

    row_counts(files)
    #explore_patients()
    explore_encounters()
    #explore_conditions()
    # explore_procedures()
    # explore_providers()
    # explore_organizations()
    # explore_claims()
    # explore_claims_transactions()
    # verify_encounter_claims_join()




if __name__ == '__main__':
    main()