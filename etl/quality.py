"""
quality.py
----------
Quality gate — runs structural/referential checks on transformed
DataFrames BEFORE load. Raises DataQualityError to halt the pipeline
on failure; logs a summary when all checks pass.

Scope (per project decision): structural/referential checks only —
nulls, referential integrity, type/range validation. Not over-engineered
for messiness Synthea doesn't have.
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
  """Raised when a data quality check fails. Halts the pipeline."""
  pass


def check_row_count(df: pd.DataFrame, min_rows: int = 1) -> dict:
  """Fail if row count is below min_rows. (Reusable as-is.)"""
  count = len(df)
  return {
      "check": "row_count",
      "passed": count >= min_rows,
      "detail": f"{count} rows (min: {min_rows})"
  }


def check_no_null_keys(df: pd.DataFrame, key_columns: list) -> dict:
  """
  Fail if any column in key_columns has nulls.
  TODO: loop over key_columns, sum nulls per column using .isna(),
  build a detail string reporting counts per column (not just one
  number — e.g. "patient_key: 0 nulls, provider_key: 3 nulls").
  Overall check "passed" should be True only if ALL columns have 0 nulls.
  """
  details = []
  total_bad = 0

  for col in key_columns:
    bad = int(df[col].isna().sum())
    total_bad += bad 
    details.append(f"{col}: {bad} nulls") 
  return {
    "check":"no_null_keys",
    "passed":total_bad == 0,
    "detail": ", ".join(details)
  }


def check_no_negative_values(df: pd.DataFrame, columns: list) -> dict:
  """
  Fail if any column in `columns` has negative values.
  TODO: similar loop structure to check_no_null_keys, but check
  (df[col] < 0).sum() instead of .isna().sum().
  Think about which columns actually need this: total_claim_cost
  (fact_encounters), base_cost (bridge_encounter_procedure).
  """
  details = []
  total_bad = 0

  for col in columns:
    bad = int((df[col] < 0).sum())
    total_bad += bad
    details.append(f"{col}: {bad} negative values")

  return {
    "check":"no negative values",
    "passed":total_bad == 0,
    "detail":",".join(details)
  }

def check_referential_integrity(df: pd.DataFrame, key_column: str, valid_keys: pd.Series, label: str) -> dict:
  """
  Fail if any value in df[key_column] doesn't exist in valid_keys.
  TODO: think about how you'd check "is every value in this column
  present in this other set of valid values" — look at how
  _drop_unmatched in transform.py already solves a similar problem
  via merge, though here you're just checking, not dropping.
  Hint: df[key_column].isin(valid_keys) gives you a boolean Series.
  """
  missing = ~df[key_column].isin(valid_keys)
  bad = int(missing.sum())

  return {
    "check": "referential_integrity",
    "passed": bad == 0,
    "detail": f"{bad} rows with {key_column} not found in {label}"
  }


def run_quality_checks(df: pd.DataFrame, table_name: str, key_columns: list = None,cost_columns: list = None) -> dict:
  if df.empty:
    logger.info(f"No rows to check for {table_name} — skipping quality gate")
    return {"passed": True, "checks": [], "row_count": 0}

  checks = [check_row_count(df)]

  if key_columns:
    checks.append(check_no_null_keys(df, key_columns))

  if cost_columns:
    checks.append(check_no_negative_values(df, cost_columns))

  failed = [c for c in checks if not c["passed"]]
  if failed:
    first = failed[0]
    raise DataQualityError(
      f"Quality check failed on {table_name}: {first['check']} — {first['detail']}"
    )

  summary = {
    "passed": True,
    "checks": checks,
    "row_count": len(df)
  }

  logger.info(f"Quality gate passed for {table_name}: {len(df):,} rows, {len(checks)} checks")
  for c in checks:
    logger.debug(f"  {c['check']}: {c['detail']}")

  return summary
