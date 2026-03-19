"""
Pull up to 10 random positions per role_k10000_v3 from ClickHouse,
keeping only rows that have a non-empty description.

Output: role_stratified_sample.csv
  Columns: position_id, description, role_k1500_v3, role_k5000_v3, role_k10000_v3

Feed this CSV into gemma_combined_classification.py, then run
assign_unmatched_roles.ipynb to build the full role→SASB mapping.
"""

import clickhouse_connect
import pandas as pd
from pathlib import Path

BASE        = Path("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs")
OUT_PATH    = BASE / "temp_data" / "role_stratified_sample.csv"
JOBS_PER_ROLE = 10
ROLE_COL      = "role_k10000_v3"

def connect():
    client = clickhouse_connect.get_client(
        host="192.168.204.128", port=8123,
        username="default", password="pm19951014",
        connect_timeout=600, send_receive_timeout=600,
    )
    client.command("USE revelio071625")
    return client

print("Connecting to ClickHouse ...")
client = connect()

print(f"Step 1: sampling position_ids (no description) ...")
ids_df = client.query_df(f"""
    SELECT
        position_id,
        title_raw,
        role_k1500_v3,
        role_k5000_v3,
        role_k10000_v3
    FROM revelio071625.temp_processed_global_position
    WHERE description != ''
      AND toYear(parseDateTimeBestEffortOrNull(startdate)) BETWEEN 2007 AND 2025
    ORDER BY role_k10000_v3, rand()
    LIMIT {JOBS_PER_ROLE} BY role_k10000_v3
""")
print(f"  Sampled {len(ids_df):,} position_ids across {ids_df['role_k10000_v3'].nunique()} roles")

print(f"Step 2: fetching descriptions for sampled IDs (in batches) ...")
all_ids   = ids_df["position_id"].tolist()
BATCH     = 5_000
desc_parts = []
for i in range(0, len(all_ids), BATCH):
    chunk   = all_ids[i : i + BATCH]
    id_list = ", ".join(str(x) for x in chunk)
    part    = client.query_df(f"""
        SELECT position_id, description
        FROM revelio071625.temp_processed_global_position
        WHERE position_id IN ({id_list})
    """)
    desc_parts.append(part)
    print(f"  fetched batch {i//BATCH + 1}/{(len(all_ids)-1)//BATCH + 1} ({len(part):,} rows)")
desc_df = pd.concat(desc_parts, ignore_index=True)

df = ids_df.merge(desc_df, on="position_id", how="left")

print(f"  Pulled {len(df):,} rows")
print(f"  Unique role_k10000_v3 covered: {df['role_k10000_v3'].nunique()}")
print(f"  Jobs per role distribution:")
counts = df['role_k10000_v3'].value_counts()
print(f"    Roles with exactly {JOBS_PER_ROLE} jobs: {(counts == JOBS_PER_ROLE).sum()}")
print(f"    Roles with < {JOBS_PER_ROLE} jobs (rare roles): {(counts < JOBS_PER_ROLE).sum()}")
print(f"    Median: {counts.median():.0f}, Min: {counts.min()}, Max: {counts.max()}")

OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT_PATH, index=False)
print(f"\nSaved to: {OUT_PATH}")
