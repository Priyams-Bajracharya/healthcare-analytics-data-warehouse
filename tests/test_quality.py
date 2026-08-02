import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import pandas as pd
from etl.quality import run_quality_checks, DataQualityError

# Build a small fake fact_encounters-like DataFrame with a bad row
bad_df = pd.DataFrame({
  "patient_key": [1, 2, None],       # <- null key, should fail check_no_null_keys
  "provider_key": [1, 2, 3],
  "organization_key": [1, 2, 3],
  "total_claim_cost": [100.0, 50.0, -25.0],  # <- negative cost, should also fail
})

try:
  run_quality_checks(bad_df, "fake_fact_encounters",                key_columns=["patient_key", "provider_key", "organization_key"],                    cost_columns=["total_claim_cost"])
  print("ERROR: quality gate did not raise — this is a bug in quality.py!")
except DataQualityError as e:
  print(f"PASS: quality gate correctly caught the bad data -> {e}")