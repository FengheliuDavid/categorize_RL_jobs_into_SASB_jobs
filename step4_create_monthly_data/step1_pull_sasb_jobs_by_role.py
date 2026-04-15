"""
Pull all positions whose role_k10000_v3 maps to at least one SASB category,
using the role-to-SASB mapping produced by step3_assign_unmatched_roles.

Assumes temp_processed_global_position already exists in ClickHouse
(created by OTHER_METHODS/keyword_dictionary_approach/step1_pull_new_sasb_jobs.py).

Saves one parquet per year to:
  temp_data/step4_create_monthly_data/positions_by_year/positions_{year}.parquet

Columns saved: position_id, rcid, startdate, role_k10000_v3, weight
"""

import ast
import clickhouse_connect
import pandas as pd
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
BASE      = Path("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs")
TEMP_DATA = BASE / "temp_data"

THRESHOLD    = "10m_0.3"
ROLE_MAPPING = TEMP_DATA / "step3_assign_unmatched_roles" / f"role_to_sasb_mapping_{THRESHOLD}.csv"
OUT_DIR      = TEMP_DATA / "step4_create_monthly_data" / f"positions_by_year_{THRESHOLD}"
OUT_DIR.mkdir(parents=True, exist_ok=True)

YEARS = list(range(2007, 2026))

# ── Helpers ───────────────────────────────────────────────────────────────────
def parse_list(val):
    if isinstance(val, list): return val
    if isinstance(val, str) and val.strip():
        try: return ast.literal_eval(val)
        except: return []
    return []

def connect():
    client = clickhouse_connect.get_client(
        host="192.168.204.128", port=8123,
        username="default", password="pm19951014",
        connect_timeout=600, send_receive_timeout=600,
    )
    client.command("USE revelio071625")
    return client

# ── 1. Load role mapping — keep only roles with ≥1 SASB category ──────────────
print("Loading role-to-SASB mapping ...")
mapping = pd.read_csv(ROLE_MAPPING)
mapping["sasb_categories"] = mapping["sasb_categories"].apply(parse_list)
mapped_roles = mapping.loc[mapping["sasb_categories"].apply(len) > 0, "role_k10000_v3"].tolist()
print(f"  Roles with SASB mapping: {len(mapped_roles):,}")


# ── 2. Create tmp_mapped_roles in ClickHouse ──────────────────────────────────
print("Connecting to ClickHouse ...")
client = connect()

print("  Creating tmp_mapped_roles ...")
client.command("DROP TABLE IF EXISTS revelio071625.tmp_mapped_roles")
client.command("""
    CREATE TABLE revelio071625.tmp_mapped_roles (role String) ENGINE = Memory
""")
# Use client.insert() to avoid manual SQL escaping
rows = [[r] for r in mapped_roles]
client.insert("revelio071625.tmp_mapped_roles", rows, column_names=["role"])
print(f"  Inserted {len(rows):,} roles")

# ── 3. Pull positions year by year ────────────────────────────────────────────
# temp_processed_global_position already exists (created by step1_pull_new_sasb_jobs.py)
# and is already filtered to in-universe RCIDs — no need to rejoin tmp_global_rcids.
for year in YEARS:
    out_path = OUT_DIR / f"positions_{year}.parquet"
    if out_path.exists():
        print(f"  {year}: already exists, skipping")
        continue

    print(f"  Pulling {year} ...")
    query = f"""
    SELECT
        p.position_id,
        p.rcid,
        p.startdate,
        p.role_k10000_v3,
        p.weight
    FROM revelio071625.temp_processed_global_position p
    INNER JOIN revelio071625.tmp_mapped_roles m ON p.role_k10000_v3 = m.role
    WHERE toYear(parseDateTimeBestEffortOrNull(p.startdate)) = {year}
    """
    df = client.query_df(query)
    df.to_parquet(out_path, index=False)
    print(f"    → {len(df):,} rows saved to {out_path.name}")

print("\nDone.")
