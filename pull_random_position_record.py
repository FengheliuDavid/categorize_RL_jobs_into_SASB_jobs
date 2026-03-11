"""
Pull 10,000 randomly sampled positions from temp_processed_global_position
(startdate 2007–2025), merge with dictionary-based SASB classification from
step1 output CSVs, and compare with LLM-based classification in output_sasb.csv.

Output: comparison_sasb.csv with columns:
    position_id, startdate, enddate, rcid, title_raw,
    role_k1500_v3, role_k5000_v3, role_k10000_v3,
    dict_sasb_categories,   ← from position_table_simple / _regex CSVs
    llm_sasb_categories,    ← from output_sasb.csv (joined on 3 role columns)
    llm_confidence
"""

import ast
import clickhouse_connect
import pandas as pd
from pathlib import Path
from pandas.errors import EmptyDataError

# ── Config ───────────────────────────────────────────────────────────────────
N_SAMPLE   = 10_000
BASE       = Path("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs")
# BASE       = Path("D:/Dropbox/fengheliu/temp/sasb_jobs")
SIMPLE_DIR = BASE / "position_table_simple"
REGEX_DIR  = BASE / "position_table_regex"
OUT_SASB   = BASE / "output_sasb.csv"
OUT_PATH   = BASE / "comparison_sasb.csv"

# ── ClickHouse ────────────────────────────────────────────────────────────────
def connect():
    client = clickhouse_connect.get_client(
        host="192.168.204.128", port=8123,
        username="default", password="YOUR_PASSWORD_HERE",
        connect_timeout=600, send_receive_timeout=600,
    )
    client.command("USE revelio071625")
    return client

# ── 1. Pull random sample from DB ────────────────────────────────────────────
print("Pulling random sample from DB ...")
client = connect()
sample_df = client.query_df(f"""
    SELECT
        position_id,
        startdate,
        enddate,
        rcid,
        title_raw,
        role_k1500_v3,
        role_k5000_v3,
        role_k10000_v3, 
        description                    
    FROM temp_processed_global_position
    WHERE toYear(parseDateTimeBestEffortOrNull(startdate)) BETWEEN 2007 AND 2025
    ORDER BY rand()
    LIMIT {N_SAMPLE}
""")
# check 
print(f"  Sampled {len(sample_df):,} positions")

# ── 2. Load step1 classification CSVs ────────────────────────────────────────
print("Loading step1 classified CSVs ...")
parts = []

for f_simple in sorted(SIMPLE_DIR.glob("rl_sasb_raw_*.csv")):
    stem     = f_simple.stem                  # e.g. rl_sasb_raw_GHG_Emissions_2015
    category = stem[len("rl_sasb_raw_"):-5]   # strip prefix + _YYYY
    f_regex  = REGEX_DIR / f_simple.name

    for fpath in (f_simple, f_regex):
        if not fpath.exists() or fpath.stat().st_size == 0:
            continue
        try:
            df = pd.read_csv(fpath, usecols=["position_id"])
            df["category"] = category
            parts.append(df)
        except EmptyDataError:
            pass

all_classified = pd.concat(parts, ignore_index=True)
print(f"  Total classified rows across all files: {len(all_classified):,}")

# Aggregate per position_id → sorted list of unique SASB categories
dict_df = (
    all_classified
    .groupby("position_id", sort=False)["category"]
    .agg(lambda x: sorted(x.dropna().unique().tolist()))
    .reset_index()
    .rename(columns={"category": "dict_sasb_categories"})
)
print(f"  Unique classified positions: {len(dict_df):,}")
dict_df.to_csv(BASE / "temp_data" / "dict_df.csv")

# ── 3. Merge dict classification onto sample ──────────────────────────────────
print("Merging dict classification onto sample ...")
merged = sample_df.merge(dict_df, on="position_id", how="left")
merged["dict_sasb_categories"] = merged["dict_sasb_categories"].apply(
    lambda x: x if isinstance(x, list) else []
)
n_dict = (merged["dict_sasb_categories"].apply(len) > 0).sum()
print(f"  Positions with dict classification: {n_dict:,} / {len(merged):,}")
merged.to_csv(BASE / "temp_data" / "merged.csv")

# ── 4. Load & merge LLM classification ───────────────────────────────────────
print("Loading output_sasb.csv ...")
llm_df = pd.read_csv(OUT_SASB)

def parse_list_col(s):
    if isinstance(s, list):
        return s
    try:
        return ast.literal_eval(s)
    except Exception:
        return []

llm_df["sasb_categories"] = llm_df["sasb_categories"].apply(parse_list_col)
llm_df = llm_df.rename(columns={
    "sasb_categories": "llm_sasb_categories",
    "confidence":      "llm_confidence",
})

# Join on role_k10000_v3: output_sasb.csv was classified at this level, so this gives
# the exact per-role LLM result rather than a spurious union across broader role buckets.
JOIN_COL = "role_k10000_v3"
llm_agg = (
    llm_df.groupby(JOIN_COL, dropna=False)
    .agg(
        llm_sasb_categories=(
            "llm_sasb_categories",
            lambda x: sorted({
                cat
                for item in x
                for cat in (ast.literal_eval(item) if isinstance(item, str) else item)
                if isinstance(cat, str) and cat
            }),
        ),
        llm_confidence=("llm_confidence", "first"),
    )
    .reset_index()
)

print("Merging LLM classification onto sample (joining on role_k10000_v3) ...")
merged = merged.merge(llm_agg, on=JOIN_COL, how="left")
merged["llm_sasb_categories"] = merged["llm_sasb_categories"].apply(
    lambda x: x if isinstance(x, list) else []
)
n_llm = (merged["llm_sasb_categories"].apply(len) > 0).sum()
print(f"  Positions with LLM classification:  {n_llm:,} / {len(merged):,}")

# ── 5. Save ───────────────────────────────────────────────────────────────────
col_order = [
    "position_id", "startdate", "enddate", "rcid", "title_raw",
    "role_k1500_v3", "role_k5000_v3", "role_k10000_v3",
    "dict_sasb_categories", "llm_sasb_categories", "llm_confidence", "description"
]
merged[col_order].to_csv(OUT_PATH, index=False)
print(f"\nSaved {len(merged):,} rows to:\n  {OUT_PATH}")



# ── 6. Summary table: hit rate by method and SASB category ───────────────────
print("\nGenerating summary table ...")

df = merged.copy()
df["dict_sasb_categories"] = df["dict_sasb_categories"].apply(parse_list_col)
df["llm_sasb_categories"]  = df["llm_sasb_categories"].apply(parse_list_col)

all_categories = sorted({
    cat
    for col in ("dict_sasb_categories", "llm_sasb_categories")
    for cats in df[col]
    for cat in cats
})

n = len(df)
rows = []
for cat in all_categories:
    dict_hit = df["dict_sasb_categories"].apply(lambda x: cat in x)
    llm_hit  = df["llm_sasb_categories"].apply(lambda x: cat in x)
    rows.append({
        "sasb_category":   cat,
        "dict_n":          dict_hit.sum(),
        "dict_rate_%":     round(dict_hit.mean() * 100, 3),
        "llm_n":           llm_hit.sum(),
        "llm_rate_%":      round(llm_hit.mean() * 100, 3),
        "both_n":          (dict_hit & llm_hit).sum(),
        "only_dict_n":     (dict_hit & ~llm_hit).sum(),
        "only_llm_n":      (~dict_hit & llm_hit).sum(),
    })

# totals row (any category assigned)
dict_any = df["dict_sasb_categories"].apply(lambda x: len(x) > 0)
llm_any  = df["llm_sasb_categories"].apply(lambda x: len(x) > 0)
rows.append({
    "sasb_category": "TOTAL (any category)",
    "dict_n":        dict_any.sum(),
    "dict_rate_%":   round(dict_any.mean() * 100, 3),
    "llm_n":         llm_any.sum(),
    "llm_rate_%":    round(llm_any.mean() * 100, 3),
    "both_n":        (dict_any & llm_any).sum(),
    "only_dict_n":   (dict_any & ~llm_any).sum(),
    "only_llm_n":    (~dict_any & llm_any).sum(),
})

summary = pd.DataFrame(rows)
SUMMARY_PATH = BASE / "summary_hit_rate.csv"
summary.to_csv(SUMMARY_PATH, index=False)
print(summary.to_string(index=False))
print(f"\nSaved summary to:\n  {SUMMARY_PATH}")
