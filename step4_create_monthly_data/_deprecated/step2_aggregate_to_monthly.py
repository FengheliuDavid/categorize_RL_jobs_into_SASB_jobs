"""
Aggregate role-based SASB positions to rcid-month level.

Inputs:
  - temp_data/step4_create_monthly_data/positions_by_year/positions_{year}.parquet
  - temp_data/step3_assign_unmatched_roles/role_to_sasb_mapping_10m_0.3.csv
  - cleaned_data/keyword_dictionary_approach/reveliolab_universe_identifiers.csv
  - cleaned_data/keyword_dictionary_approach/all_new_jobs_monthly.csv

Outputs (in cleaned_data/role_based_approach/):
  - revelio_sasb_monthly_new_jobs_role_based.parquet   (rcid × month × 26 SASB counts)
  - revelio_sasb_monthly_share_new_jobs_role_based.parquet  (pct_ columns)
"""

import ast
import numpy as np
import pandas as pd
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
BASE      = Path("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs")
TEMP_DATA = BASE / "temp_data"

THRESHOLD       = "10m_0.3"
ROLE_MAPPING    = TEMP_DATA / "step3_assign_unmatched_roles" / f"role_to_sasb_mapping_{THRESHOLD}.csv"
POSITIONS_DIR   = TEMP_DATA / "step4_create_monthly_data" / f"positions_by_year_{THRESHOLD}"
UNIVERSE        = BASE / "cleaned_data" / "keyword_dictionary_approach" / "reveliolab_universe_identifiers.csv"
ALL_JOBS        = BASE / "cleaned_data" / "keyword_dictionary_approach" / "all_new_jobs_monthly.csv"
OUT_DIR         = BASE / "cleaned_data" / "role_based_approach_w_10m_description_fallback"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SASB_COLS = [
    'Access_&_Affordability', 'Air_Quality',
    'Business_Ethics', 'Business_Model_Resilience', 'Competitive_Behavior',
    'Critical_Incident_Risk_Management', 'Customer_Privacy',
    'Customer_Welfare', 'Data_Security', 'Ecological_Impacts',
    'Employee_Engagement,_Diversity_&_Inclusion',
    'Employee_Health_&_Safety', 'Energy_Management', 'GHG_Emissions',
    'Human_Rights_&_Community_Relations', 'Labor_Practices',
    'Management_of_the_Legal_&_Regulatory_Environment',
    'Materials_Sourcing_&_Efficiency', 'Physical_Impacts_of_Climate_Change',
    'Product_Design_&_Lifecycle_Management', 'Product_Quality_&_Safety',
    'Selling_Practices_&_Product_Labeling', 'Supply_Chain_Management',
    'Systemic_Risk_Management', 'Waste_&_Hazardous_Materials_Management',
    'Water_&_Wastewater_Management',
]

def parse_list(val):
    if isinstance(val, list): return val
    if isinstance(val, str) and val.strip():
        try: return ast.literal_eval(val)
        except: return []
    return []

# ── 1. Load role mapping ───────────────────────────────────────────────────────
print("Loading role-to-SASB mapping ...")
mapping = pd.read_csv(ROLE_MAPPING, usecols=["role_k10000_v3", "sasb_categories"])
mapping["sasb_categories"] = mapping["sasb_categories"].apply(parse_list)
mapping = mapping[mapping["sasb_categories"].apply(len) > 0].reset_index(drop=True)
print(f"  {len(mapping):,} roles with SASB mapping")

# ── 2. Load all year parquets ─────────────────────────────────────────────────
print("Loading position parquets ...")
parts = []
for f in sorted(POSITIONS_DIR.glob("positions_*.parquet")):
    df = pd.read_parquet(f)
    parts.append(df)
    print(f"  {f.name}: {len(df):,} rows")

positions = pd.concat(parts, ignore_index=True)
print(f"Total positions: {len(positions):,}")

# ── 3. Join SASB categories ───────────────────────────────────────────────────
print("Joining SASB categories ...")
positions = positions.merge(mapping, on="role_k10000_v3", how="inner")

# ── 4. Parse startdate -> start_month ─────────────────────────────────────────
print("Parsing dates ...")
positions["startdate"] = pd.to_datetime(positions["startdate"], errors="coerce")
positions = positions.dropna(subset=["startdate"])
positions["start_month"] = positions["startdate"].dt.to_period("M").astype(str)

# ── 5. Explode sasb_categories -> one row per position × category ──────────────
print("Exploding SASB categories ...")
positions["sasb_categories"] = positions["sasb_categories"].apply(parse_list)
positions = positions.explode("sasb_categories").rename(columns={"sasb_categories": "category"})
positions = positions.dropna(subset=["category"])
print(f"  {len(positions):,} position-category rows")

# ── 6. Aggregate: rcid × start_month × category -> weighted count ─────────────
print("Aggregating to rcid-month-category ...")
long = (
    positions
    .groupby(["rcid", "start_month", "category"], as_index=False)["weight"]
    .sum()
    .rename(columns={"weight": "job_weighted_count"})
)

# ── 7. Pivot to wide format ───────────────────────────────────────────────────
print("Pivoting to wide format ...")
wide = (
    long.pivot_table(
        index=["rcid", "start_month"],
        columns="category",
        values="job_weighted_count",
        aggfunc="sum",
        fill_value=0,
    )
    .reset_index()
)
wide.columns.name = None

# Ensure all 26 SASB columns exist
for col in SASB_COLS:
    if col not in wide.columns:
        print(col)
        wide[col] = 0

# ── 8. Balance panel: all rcid × all months 2007-01 to 2025-12 ───────────────
print("Building balanced panel ...")
all_jobs = pd.read_csv(ALL_JOBS)
if "month" in all_jobs.columns and "start_month" not in all_jobs.columns:
    all_jobs = all_jobs.rename(columns={"month": "start_month"})

all_months = pd.DataFrame({
    "start_month": pd.period_range("2007-01", "2025-12", freq="M").astype(str)
})
df_link = pd.read_csv(UNIVERSE).drop_duplicates(subset=["rcid"])
rcids = pd.DataFrame({"rcid": all_jobs["rcid"].unique()})
panel = rcids.merge(all_months, how="cross")

wide_balanced = panel.merge(wide, on=["rcid", "start_month"], how="left")
for col in SASB_COLS:
    wide_balanced[col] = wide_balanced[col].fillna(0).astype("float32")
print(f"  Balanced panel: {len(wide_balanced):,} rows, {wide_balanced['rcid'].nunique():,} RCIDs")

# ── 9. Merge company identifiers ──────────────────────────────────────────────
print("Merging company identifiers ...")
wide_balanced = wide_balanced.merge(df_link, on="rcid", how="left")
wide_balanced["gvkey"].isna().sum()
len(wide_balanced)

# ── 10. Save monthly counts ───────────────────────────────────────────────────
out_counts = OUT_DIR / f"revelio_sasb_monthly_new_jobs_role_based_{THRESHOLD}.parquet"
wide_balanced.to_parquet(out_counts, index=False)
print(f"Saved counts -> {out_counts}")

# ── 11. Compute shares ────────────────────────────────────────────────────────
print("Computing SASB shares ...")

df_share = wide_balanced.merge(all_jobs[["rcid", "start_month", "all_new_jobs_weighted"]],
                                on=["rcid", "start_month"], how="left")

for col in SASB_COLS:
    df_share[f"pct_{col}"] = np.where(
        df_share["all_new_jobs_weighted"] > 0,
        df_share[col] / df_share["all_new_jobs_weighted"],
        np.nan,
    )

pct_cols = [f"pct_{c}" for c in SASB_COLS]
df_share[pct_cols] = df_share[pct_cols].fillna(0)

out_share = OUT_DIR / f"revelio_sasb_monthly_share_new_jobs_role_based_{THRESHOLD}.parquet"
df_share.to_parquet(out_share, index=False)
print(f"Saved shares  -> {out_share}")

# ── 12. Quick summary ─────────────────────────────────────────────────────────
print("\n-- Summary --")
print(f"  RCIDs with any SASB job : {wide_balanced['rcid'].nunique():,}")
print(f"  Total rcid-months       : {len(wide_balanced):,}")
any_sasb = (wide_balanced[SASB_COLS].sum(axis=1) > 0).sum()
print(f"  rcid-months with >0 jobs: {any_sasb:,} ({any_sasb/len(wide_balanced)*100:.1f}%)")
print("\nTop categories by total weighted jobs:")
totals = wide_balanced[SASB_COLS].sum().sort_values(ascending=False)
print(totals.to_string())
