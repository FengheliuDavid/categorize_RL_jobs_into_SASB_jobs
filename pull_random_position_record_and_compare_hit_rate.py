"""
Compare hit rates for three SASB classification approaches on a 10k random job sample.

  gemma_role : step1 — role-title LLM (gemma_role_classification_output.csv)
  gemma_desc : step2 — description-based LLM (gemma_combined_classification_output_100k.csv)
  two_step   : step3 — combined role+desc mapping (role_to_sasb_mapping.csv)

Uses cached merged.csv (sample + dict classification). Does NOT require ClickHouse.
"""

import ast
import pandas as pd
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
BASE      = Path("D:/Dropbox/fengheliu/temp/sasb_jobs")
TEMP_DATA = BASE / "temp_data"
COMP_DIR  = BASE / "COMPARISON"
COMP_DIR.mkdir(exist_ok=True)

MERGED_CACHE = TEMP_DATA / "merged.csv"
STEP1_CSV    = TEMP_DATA / "step1_gemma_role_classification" / "gemma_role_classification_output.csv"
STEP2_CSV    = TEMP_DATA / "step2_gemma_combined_classification" / "gemma_combined_classification_output_100k.csv"
STEP2_SAMPLE = TEMP_DATA / "step2_gemma_combined_classification" / "role_stratified_sample.csv"
STEP3_CSV    = TEMP_DATA / "step3_assign_unmatched_roles" / "role_to_sasb_mapping.csv"
OUT_PATH     = COMP_DIR / "comparison_sasb_v3.csv"
SUMMARY_PATH = COMP_DIR / "summary_hit_rate_v3.csv"

ROLE_COL = "role_k10000_v3"

def parse_list(val):
    if isinstance(val, list): return val
    if isinstance(val, str) and val.strip():
        try: return ast.literal_eval(val)
        except: return []
    return []

# ── 1. Load cached sample ─────────────────────────────────────────────────────
print("Loading cached merged.csv ...")
merged = pd.read_csv(MERGED_CACHE)
merged["dict_sasb_categories"] = merged["dict_sasb_categories"].apply(parse_list)
n_dict = (merged["dict_sasb_categories"].apply(len) > 0).sum()
print(f"  {len(merged):,} rows; dict-classified: {n_dict:,} ({n_dict/len(merged)*100:.2f}%)")

# ── 2. gemma_role (step1): aggregate by role_k10000_v3 ───────────────────────
print("\nLoading step1 (gemma_role) ...")
role_df = pd.read_csv(STEP1_CSV)
role_df["sasb_categories"] = role_df["sasb_categories"].apply(parse_list)
role_df = role_df[role_df[ROLE_COL].notna()]

role_map = (
    role_df.groupby(ROLE_COL)["sasb_categories"]
    .agg(lambda x: sorted({cat for cats in x for cat in cats}))
    .reset_index()
    .rename(columns={"sasb_categories": "gemma_role_categories"})
)
merged = merged.merge(role_map, on=ROLE_COL, how="left")
merged["gemma_role_categories"] = merged["gemma_role_categories"].apply(
    lambda x: x if isinstance(x, list) else []
)
n_role = (merged["gemma_role_categories"].apply(len) > 0).sum()
print(f"  Positions with gemma_role: {n_role:,} / {len(merged):,} ({n_role/len(merged)*100:.2f}%)")

# ── 3. gemma_desc (step2): join sample→role, aggregate by role_k10000_v3 ─────
print("\nLoading step2 (gemma_desc) ...")
step2_sample = pd.read_csv(STEP2_SAMPLE, usecols=["position_id", ROLE_COL])
step2_llm    = pd.read_csv(STEP2_CSV, usecols=["position_id", "sasb_categories"])
step2_llm["sasb_categories"] = step2_llm["sasb_categories"].apply(parse_list)

step2_joined = step2_sample.merge(step2_llm, on="position_id", how="inner")
desc_map = (
    step2_joined.groupby(ROLE_COL)["sasb_categories"]
    .agg(lambda x: sorted({cat for cats in x for cat in cats}))
    .reset_index()
    .rename(columns={"sasb_categories": "gemma_desc_categories"})
)
merged = merged.merge(desc_map, on=ROLE_COL, how="left")
merged["gemma_desc_categories"] = merged["gemma_desc_categories"].apply(
    lambda x: x if isinstance(x, list) else []
)
n_desc = (merged["gemma_desc_categories"].apply(len) > 0).sum()
print(f"  Positions with gemma_desc: {n_desc:,} / {len(merged):,} ({n_desc/len(merged)*100:.2f}%)")

# ── 4. two_step (step3): role_to_sasb_mapping, join by role_k10000_v3 ─────────
print("\nLoading step3 (two_step) ...")
step3 = pd.read_csv(STEP3_CSV, usecols=[ROLE_COL, "sasb_categories"])
step3["sasb_categories"] = step3["sasb_categories"].apply(parse_list)
step3 = step3.rename(columns={"sasb_categories": "two_step_categories"})

merged = merged.merge(step3, on=ROLE_COL, how="left")
merged["two_step_categories"] = merged["two_step_categories"].apply(
    lambda x: x if isinstance(x, list) else []
)
n_two = (merged["two_step_categories"].apply(len) > 0).sum()
print(f"  Positions with two_step:   {n_two:,} / {len(merged):,} ({n_two/len(merged)*100:.2f}%)")

# ── 5. Save comparison CSV ────────────────────────────────────────────────────
col_order = [
    "position_id", "startdate", "enddate", "rcid", "title_raw", "description",
    "role_k1500_v3", "role_k5000_v3", ROLE_COL,
    "dict_sasb_categories",
    "gemma_role_categories",
    "gemma_desc_categories",
    "two_step_categories",
]
col_order = [c for c in col_order if c in merged.columns]
merged[col_order].to_csv(OUT_PATH, index=False)
print(f"\nSaved {len(merged):,} rows to: {OUT_PATH}")

# ── 6. Summary table ──────────────────────────────────────────────────────────
print("\nGenerating summary table ...")

SUMMARY_COLS = [
    ("dict_sasb_categories",  "dict"),
    ("gemma_role_categories", "gemma_role"),
    ("gemma_desc_categories", "gemma_desc"),
    ("two_step_categories",   "two_step"),
]

all_cats = sorted({
    cat
    for col, _ in SUMMARY_COLS
    for cats in merged[col]
    for cat in cats
})

rows = []
total = len(merged)
for cat in all_cats:
    row = {"sasb_category": cat}
    for col, prefix in SUMMARY_COLS:
        hit = merged[col].apply(lambda x, c=cat: c in x)
        row[f"{prefix}_n"]      = int(hit.sum())
        row[f"{prefix}_rate_%"] = round(hit.mean() * 100, 3)
    rows.append(row)

# TOTAL row
row = {"sasb_category": "TOTAL (any category)"}
for col, prefix in SUMMARY_COLS:
    hit = merged[col].apply(lambda x: len(x) > 0)
    row[f"{prefix}_n"]      = int(hit.sum())
    row[f"{prefix}_rate_%"] = round(hit.mean() * 100, 3)
rows.append(row)

summary = pd.DataFrame(rows)
summary.to_csv(SUMMARY_PATH, index=False)
print(summary.to_string(index=False))
print(f"\nSaved summary to: {SUMMARY_PATH}")
