"""
Aggregate role-based SASB positions to rcid-month level (NET inflow).

Net inflow = weighted inflow (by startdate) minus weighted outflow (by enddate),
applied to both the 26 SASB numerators and the total-jobs denominator.

Inputs:
  - temp_data/step4_create_monthly_data/positions_by_year_{threshold}/positions_{year}.parquet
  - temp_data/step3_assign_unmatched_roles/role_to_sasb_mapping_{threshold}.csv
  - cleaned_data/keyword_dictionary_approach/reveliolab_universe_identifiers.csv
  - cleaned_data/keyword_dictionary_approach/all_new_jobs_monthly.csv      (total inflow)
  - cleaned_data/keyword_dictionary_approach/all_outflow_jobs_monthly.csv  (total outflow, from step1)

Outputs (in cleaned_data/role_based_approach_net_inflow/):
  - revelio_sasb_monthly_net_new_jobs_role_based_{threshold}.parquet   (rcid x month x 26 SASB net counts)
  - revelio_sasb_monthly_net_share_new_jobs_role_based_{threshold}.parquet  (pct_ columns, net/net denominator)
"""

import ast
import numpy as np
import pandas as pd
from pathlib import Path

# ── Config ─────────────────────────────────────────────────────────────────────
BASE      = Path("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs")
TEMP_DATA = BASE / "temp_data"

THRESHOLD     = "10m_0.3"
ROLE_MAPPING  = TEMP_DATA / "step3_assign_unmatched_roles" / f"role_to_sasb_mapping_{THRESHOLD}.csv"
POSITIONS_DIR         = TEMP_DATA / "step4_create_monthly_data" / f"positions_by_year_{THRESHOLD}"
OUTFLOW_POSITIONS_DIR = TEMP_DATA / "step4_create_monthly_data" / f"outflow_positions_by_year_{THRESHOLD}"
UNIVERSE      = BASE / "cleaned_data" / "keyword_dictionary_approach" / "reveliolab_universe_identifiers.csv"
ALL_INFLOW    = BASE / "cleaned_data" / "keyword_dictionary_approach" / "all_new_jobs_monthly.csv"
ALL_OUTFLOW   = BASE / "cleaned_data" / "keyword_dictionary_approach" / "all_outflow_jobs_monthly.csv"
OUT_DIR       = BASE / "cleaned_data" / "role_based_approach_net_inflow"
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

# ── 1. Load role mapping ────────────────────────────────────────────────────────
print("Loading role-to-SASB mapping ...")
mapping = pd.read_csv(ROLE_MAPPING, usecols=["role_k10000_v3", "sasb_categories"])
mapping["sasb_categories"] = mapping["sasb_categories"].apply(parse_list)
mapping = mapping[mapping["sasb_categories"].apply(len) > 0].reset_index(drop=True)
print(f"  {len(mapping):,} roles with SASB mapping")

# ── 2. Load all year parquets ──────────────────────────────────────────────────
print("Loading position parquets ...")
parts = []
for f in sorted(POSITIONS_DIR.glob("positions_*.parquet")):
    df = pd.read_parquet(f)
    parts.append(df)
    print(f"  {f.name}: {len(df):,} rows")

positions = pd.concat(parts, ignore_index=True)
print(f"Total positions: {len(positions):,}")

# ── 3. Join SASB categories ────────────────────────────────────────────────────
print("Joining SASB categories ...")
positions = positions.merge(mapping, on="role_k10000_v3", how="inner")
print(f"Total positions: {len(positions):,}")

# ── 4. Parse startdate ─────────────────────────────────────────────────────────
print("Parsing dates ...")
positions["startdate"] = pd.to_datetime(positions["startdate"], errors="coerce")
positions = positions.dropna(subset=["startdate"])
positions["start_month"] = positions["startdate"].dt.to_period("M").astype(str)

# ── 5. Explode sasb_categories ─────────────────────────────────────────────────
print("Exploding SASB categories ...")
positions["sasb_categories"] = positions["sasb_categories"].apply(parse_list)
positions = positions.explode("sasb_categories").rename(columns={"sasb_categories": "category"})
positions = positions.dropna(subset=["category"])
print(f"  {len(positions):,} position-category rows")

# ── 6. Aggregate inflow: rcid x start_month x category ────────────────────────
print("Aggregating inflow ...")
inflow_long = (
    positions
    .groupby(["rcid", "start_month", "category"], as_index=False)["weight"]
    .sum()
    .rename(columns={"weight": "inflow_count"})
)

# ── 7. Load outflow parquets, join SASB, explode, aggregate ───────────────────
# Outflow parquets are partitioned by enddate year (not startdate), so positions
# that started before the sample window but ended within it are included.
print("Loading outflow position parquets ...")
outflow_parts = []
for f in sorted(OUTFLOW_POSITIONS_DIR.glob("outflow_positions_*.parquet")):
    df = pd.read_parquet(f)
    outflow_parts.append(df)
    print(f"  {f.name}: {len(df):,} rows")

outflow_positions = pd.concat(outflow_parts, ignore_index=True)
print(f"Total outflow positions: {len(outflow_positions):,}")

outflow_positions = outflow_positions.merge(mapping, on="role_k10000_v3", how="inner")
outflow_positions["enddate"] = pd.to_datetime(outflow_positions["enddate"], errors="coerce")
outflow_positions = outflow_positions.dropna(subset=["enddate"])
outflow_positions["end_month"] = outflow_positions["enddate"].dt.to_period("M").astype(str)
outflow_positions["sasb_categories"] = outflow_positions["sasb_categories"].apply(parse_list)
outflow_positions = outflow_positions.explode("sasb_categories").rename(columns={"sasb_categories": "category"})
outflow_positions = outflow_positions.dropna(subset=["category"])

print("Aggregating outflow ...")
outflow_long = (
    outflow_positions
    .groupby(["rcid", "end_month", "category"], as_index=False)["weight"]
    .sum()
    .rename(columns={"weight": "outflow_count", "end_month": "start_month"})
)

# ── 8. Pivot inflow and outflow to wide ────────────────────────────────────────
print("Pivoting to wide format ...")
inflow_wide = (
    inflow_long.pivot_table(
        index=["rcid", "start_month"],
        columns="category",
        values="inflow_count",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()
)
inflow_wide.columns.name = None

outflow_wide = (
    outflow_long.pivot_table(
        index=["rcid", "start_month"],
        columns="category",
        values="outflow_count",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()
)
outflow_wide.columns.name = None

# Ensure all 26 SASB columns exist in both tables before subtraction
for col in SASB_COLS:
    if col not in inflow_wide.columns:
        inflow_wide[col] = 0
    if col not in outflow_wide.columns:
        outflow_wide[col] = 0

# ── 9. Compute net = inflow - outflow ──────────────────────────────────────────
print("Computing net inflow ...")
net_wide = (
    inflow_wide[["rcid", "start_month"] + SASB_COLS]
    .merge(
        outflow_wide[["rcid", "start_month"] + SASB_COLS],
        on=["rcid", "start_month"],
        how="outer",
        suffixes=("_in", "_out"),
    )
)
for col in SASB_COLS:
    net_wide[col] = net_wide[f"{col}_in"].fillna(0) - net_wide[f"{col}_out"].fillna(0)

net_wide = net_wide[["rcid", "start_month"] + SASB_COLS]

# ── 10. Balance panel: all rcid x all months 2007-01 to 2025-12 ───────────────
print("Building balanced panel ...")
all_inflow = pd.read_csv(ALL_INFLOW)
if "month" in all_inflow.columns and "start_month" not in all_inflow.columns:
    all_inflow = all_inflow.rename(columns={"month": "start_month"})

all_months = pd.DataFrame({
    "start_month": pd.period_range("2007-01", "2025-12", freq="M").astype(str)
})
rcids = pd.DataFrame({"rcid": all_inflow["rcid"].unique()})
panel = rcids.merge(all_months, how="cross")

net_balanced = panel.merge(net_wide, on=["rcid", "start_month"], how="left")
for col in SASB_COLS:
    net_balanced[col] = net_balanced[col].fillna(0).astype("float32")
print(f"  Balanced panel: {len(net_balanced):,} rows, {net_balanced['rcid'].nunique():,} RCIDs")

# ── 11. Merge company identifiers ──────────────────────────────────────────────
print("Merging company identifiers ...")
df_link = pd.read_csv(UNIVERSE).drop_duplicates(subset=["rcid"])
net_balanced = net_balanced.merge(df_link, on="rcid", how="left")

# ── 12. Quality check on net counts ───────────────────────────────────────────
print("\n-- Net count quality check --")
qc_counts = pd.DataFrame({
    "min":    net_balanced[SASB_COLS].min(),
    "max":    net_balanced[SASB_COLS].max(),
    "mean":   net_balanced[SASB_COLS].mean(),
    "p1":     net_balanced[SASB_COLS].quantile(0.01),
    "p99":    net_balanced[SASB_COLS].quantile(0.99),
    "pct_neg": (net_balanced[SASB_COLS] < 0).mean() * 100,
    "pct_pos": (net_balanced[SASB_COLS] > 0).mean() * 100,
    "pct_zero":(net_balanced[SASB_COLS] == 0).mean() * 100,
})
print(qc_counts.to_string())

# ── 13. Save net counts ────────────────────────────────────────────────────────
out_counts = OUT_DIR / f"revelio_sasb_monthly_net_new_jobs_role_based_{THRESHOLD}.parquet"
net_balanced.to_parquet(out_counts, index=False)
print(f"Saved net counts -> {out_counts}")

# ── 13. Compute net denominator (total inflow - total outflow) ─────────────────
print("Computing net denominator ...")
all_outflow = pd.read_csv(ALL_OUTFLOW)
if "end_month" in all_outflow.columns:
    all_outflow = all_outflow.rename(columns={"end_month": "start_month"})
if "month" in all_outflow.columns and "start_month" not in all_outflow.columns:
    all_outflow = all_outflow.rename(columns={"month": "start_month"})

net_denom = (
    all_inflow[["rcid", "start_month", "all_new_jobs_weighted"]]
    .merge(
        all_outflow[["rcid", "start_month", "all_outflow_jobs_weighted"]],
        on=["rcid", "start_month"],
        how="outer",
    )
)
net_denom["all_new_jobs_weighted"]     = net_denom["all_new_jobs_weighted"].fillna(0)
net_denom["all_outflow_jobs_weighted"] = net_denom["all_outflow_jobs_weighted"].fillna(0)
net_denom["all_net_jobs_weighted"]     = (
    net_denom["all_new_jobs_weighted"] - net_denom["all_outflow_jobs_weighted"]
)

# ── 14. Compute net shares ─────────────────────────────────────────────────────
print("Computing SASB net shares ...")
df_share = net_balanced.merge(
    net_denom[["rcid", "start_month", "all_net_jobs_weighted"]],
    on=["rcid", "start_month"],
    how="left",
)

for col in SASB_COLS:
    df_share[f"pct_{col}"] = np.where(
        df_share["all_net_jobs_weighted"] != 0,
        df_share[col] / df_share["all_net_jobs_weighted"],
        np.nan,
    )

out_share = OUT_DIR / f"revelio_sasb_monthly_net_share_new_jobs_role_based_{THRESHOLD}.parquet"
df_share.to_parquet(out_share, index=False)
print(f"Saved net shares -> {out_share}")

# ── 15. Quick summary ──────────────────────────────────────────────────────────
print("\n-- Summary --")
print(f"  RCIDs                      : {net_balanced['rcid'].nunique():,}")
print(f"  Total rcid-months          : {len(net_balanced):,}")
nonzero = (net_balanced[SASB_COLS].sum(axis=1) != 0).sum()
print(f"  rcid-months with nonzero net: {nonzero:,} ({nonzero/len(net_balanced)*100:.1f}%)")
print("\nTop categories by total net jobs:")
totals = net_balanced[SASB_COLS].sum().sort_values(ascending=False)
print(totals.to_string())

# --- data quality check ---
pct_cols = [f"pct_{c}" for c in SASB_COLS]
qc = pd.DataFrame({
    "max": df_share[pct_cols].max(),
    "min": df_share[pct_cols].min(),
})
qc.index = [c.replace("pct_", "") for c in qc.index]
print("\n-- Share range check (should be within [-1, 1]) --")
print(qc.to_string())
violations = qc[(qc["max"] > 1) | (qc["min"] < -1)]
if violations.empty:
    print("\nAll pct_ columns within [-1, 1]. OK.")
else:
    print(f"\nWARNING: {len(violations)} columns have values outside [-1, 1]:")
    print(violations.to_string())

df_share.head(1000).to_csv(OUT_DIR / f"df_share_sample_1000.csv", index=False)
net_balanced.head(1000).to_csv(OUT_DIR / f"df_net_sample_1000.csv", index=False)
