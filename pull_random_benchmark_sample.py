"""
Pull 10,000 randomly sampled positions from ClickHouse and merge with
dictionary-based SASB classification from step1 keyword CSVs.

Output: temp_data/merged.csv
  Columns: position_id, startdate, enddate, rcid, title_raw, description,
           role_k1500_v3, role_k5000_v3, role_k10000_v3, dict_sasb_categories

This file is consumed by pull_random_position_record_and_compare_hit_rate.py.
"""

import ast
import clickhouse_connect
import pandas as pd
from pathlib import Path
from pandas.errors import EmptyDataError

# ── Config ────────────────────────────────────────────────────────────────────
N_SAMPLE   = 100_000
BASE       = Path("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs")
SIMPLE_DIR = BASE / "temp_data" / "keyword_dictionary_approach" / "position_table_simple"
REGEX_DIR  = BASE / "temp_data" / "keyword_dictionary_approach" / "position_table_regex"
OUT_PATH   = BASE / "temp_data" / "merged.csv"

# ── ClickHouse ────────────────────────────────────────────────────────────────
def connect():
    client = clickhouse_connect.get_client(
        host="192.168.204.128", port=8123,
        username="default", password="pm19951014",
        connect_timeout=600, send_receive_timeout=600,
    )
    client.command("USE revelio071625")
    return client

# ── 1. Pull random sample ─────────────────────────────────────────────────────
print("Pulling random sample from DB ...")
client = connect()
sample_df = client.query_df(f"""
    SELECT
        position_id, startdate, enddate, rcid, title_raw, description,
        role_k1500_v3, role_k5000_v3, role_k10000_v3
    FROM revelio071625.temp_processed_global_position
    WHERE toYear(parseDateTimeBestEffortOrNull(startdate)) BETWEEN 2007 AND 2025
    ORDER BY rand()
    LIMIT {N_SAMPLE}
""")
print(f"  Sampled {len(sample_df):,} positions")

# ── 2. Load step1 keyword classification CSVs ─────────────────────────────────
print("Loading step1 classified CSVs ...")
parts = []
for f_simple in sorted(SIMPLE_DIR.glob("rl_sasb_raw_*.csv")):
    stem     = f_simple.stem
    category = stem[len("rl_sasb_raw_"):-5]
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
print(f"  Total classified rows: {len(all_classified):,}")

dict_df = (
    all_classified
    .groupby("position_id", sort=False)["category"]
    .agg(lambda x: sorted(x.dropna().unique().tolist()))
    .reset_index()
    .rename(columns={"category": "dict_sasb_categories"})
)
print(f"  Unique classified positions: {len(dict_df):,}")

# ── 3. Merge and save ─────────────────────────────────────────────────────────
print("Merging and saving ...")
merged = sample_df.merge(dict_df, on="position_id", how="left")
merged["dict_sasb_categories"] = merged["dict_sasb_categories"].apply(
    lambda x: x if isinstance(x, list) else []
)
n_dict = (merged["dict_sasb_categories"].apply(len) > 0).sum()
print(f"  Positions with dict classification: {n_dict:,} / {len(merged):,} ({n_dict/len(merged)*100:.2f}%)")

OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
merged.to_csv(OUT_PATH, index=False)
print(f"\nSaved to: {OUT_PATH}")
