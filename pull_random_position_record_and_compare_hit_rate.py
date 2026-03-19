"""
Pull 10,000 randomly sampled positions from temp_processed_global_position
(startdate 2007–2025), merge with dictionary-based SASB classification from
step1 output CSVs, and compare with Claude and GPT-4o classifications.

Output: comparison_sasb.csv with columns:
    position_id, startdate, enddate, rcid, title_raw, description,
    role_k1500_v3, role_k5000_v3, role_k10000_v3,
    dict_sasb_categories,    ← from position_table_simple / _regex CSVs
    claude_sasb_categories,  ← from output_sasb_claude.csv
    claude_confidence,
    gpt_sasb_categories,     ← from output_sasb_gpt.csv
    gpt_confidence
"""

import ast
import clickhouse_connect
import pandas as pd
from pathlib import Path
from pandas.errors import EmptyDataError

# ── Config ───────────────────────────────────────────────────────────────────
N_SAMPLE    = 10_000
BASE        = Path("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs")
SIMPLE_DIR  = BASE / "position_table_simple"
REGEX_DIR   = BASE / "position_table_regex"
OUT_GEMMA   = BASE / "output_sasb.csv"
OUT_CLAUDE  = BASE / "output_sasb_claude.csv"
OUT_GPT     = BASE / "output_sasb_gpt.csv"
OUT_PATH    = BASE / "comparison_sasb.csv"

# ── ClickHouse ────────────────────────────────────────────────────────────────
def connect():
    client = clickhouse_connect.get_client(
        host="192.168.204.128", port=8123,
        username="default", password="YOUR_PASSWORD_HERE",
        connect_timeout=600, send_receive_timeout=600,
    )
    client.command("USE revelio071625")
    return client

MERGED_CACHE = BASE / "temp_data" / "merged.csv"

# ── 1-3. Load cached merged.csv or rebuild from DB + step1 CSVs ──────────────
if MERGED_CACHE.exists():
    print(f"Loading cached merged.csv ...")
    merged = pd.read_csv(MERGED_CACHE)
    merged["dict_sasb_categories"] = merged["dict_sasb_categories"].apply(
        lambda x: x if isinstance(x, list) else (ast.literal_eval(x) if isinstance(x, str) else [])
    )
    n_dict = (merged["dict_sasb_categories"].apply(len) > 0).sum()
    print(f"  Loaded {len(merged):,} rows; dict-classified: {n_dict:,}")
else:
    # ── 1. Pull random sample from DB ────────────────────────────────────────
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
    print(f"  Sampled {len(sample_df):,} positions")

    # ── 2. Load step1 classification CSVs ────────────────────────────────────
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
    print(f"  Total classified rows across all files: {len(all_classified):,}")

    dict_df = (
        all_classified
        .groupby("position_id", sort=False)["category"]
        .agg(lambda x: sorted(x.dropna().unique().tolist()))
        .reset_index()
        .rename(columns={"category": "dict_sasb_categories"})
    )
    print(f"  Unique classified positions: {len(dict_df):,}")
    dict_df.to_csv(BASE / "temp_data" / "dict_df.csv")

    # ── 3. Merge dict classification onto sample ──────────────────────────────
    print("Merging dict classification onto sample ...")
    merged = sample_df.merge(dict_df, on="position_id", how="left")
    merged["dict_sasb_categories"] = merged["dict_sasb_categories"].apply(
        lambda x: x if isinstance(x, list) else []
    )
    n_dict = (merged["dict_sasb_categories"].apply(len) > 0).sum()
    print(f"  Positions with dict classification: {n_dict:,} / {len(merged):,}")
    merged.to_csv(MERGED_CACHE)

# ── Helper: load an LLM output CSV and aggregate by role_k10000_v3 ───────────
def parse_list_col(s):
    if isinstance(s, list):
        return s
    try:
        return ast.literal_eval(s)
    except Exception:
        return []

JOIN_COL = "role_k10000_v3"

def load_llm_csv(path, cat_col, conf_col):
    df = pd.read_csv(path)
    df["sasb_categories"] = df["sasb_categories"].apply(parse_list_col)
    df = df.rename(columns={"sasb_categories": cat_col, "confidence": conf_col})
    agg = (
        df.groupby(JOIN_COL, dropna=False)
        .agg(
            **{cat_col: (cat_col, lambda x: sorted({
                cat
                for item in x
                for cat in (ast.literal_eval(item) if isinstance(item, str) else item)
                if isinstance(cat, str) and cat
            }))},
            **{conf_col: (conf_col, "first")},
        )
        .reset_index()
    )
    return agg

# ── 4a. Merge Gemma classification ───────────────────────────────────────────
print("Loading and merging Gemma classification ...")
gemma_agg = load_llm_csv(OUT_GEMMA, "gemma_sasb_categories", "gemma_confidence")
merged = merged.merge(gemma_agg, on=JOIN_COL, how="left")
merged["gemma_sasb_categories"] = merged["gemma_sasb_categories"].apply(
    lambda x: x if isinstance(x, list) else []
)
n_gemma = (merged["gemma_sasb_categories"].apply(len) > 0).sum()
print(f"  Positions with Gemma classification:  {n_gemma:,} / {len(merged):,}")

# ── 4c. Merge Claude classification ──────────────────────────────────────────
print("Loading and merging Claude classification ...")
claude_agg = load_llm_csv(OUT_CLAUDE, "claude_sasb_categories", "claude_confidence")
merged = merged.merge(claude_agg, on=JOIN_COL, how="left")
merged["claude_sasb_categories"] = merged["claude_sasb_categories"].apply(
    lambda x: x if isinstance(x, list) else []
)
n_claude = (merged["claude_sasb_categories"].apply(len) > 0).sum()
print(f"  Positions with Claude classification: {n_claude:,} / {len(merged):,}")

# ── 4d. Merge GPT classification ──────────────────────────────────────────────
print("Loading and merging GPT-4o classification ...")
gpt_agg = load_llm_csv(OUT_GPT, "gpt_sasb_categories", "gpt_confidence")
merged = merged.merge(gpt_agg, on=JOIN_COL, how="left")
merged["gpt_sasb_categories"] = merged["gpt_sasb_categories"].apply(
    lambda x: x if isinstance(x, list) else []
)
n_gpt = (merged["gpt_sasb_categories"].apply(len) > 0).sum()
print(f"  Positions with GPT-4o classification: {n_gpt:,} / {len(merged):,}")

# ── 5. Save ───────────────────────────────────────────────────────────────────
col_order = [
    "position_id", "startdate", "enddate", "rcid", "title_raw", "description",
    "role_k1500_v3", "role_k5000_v3", "role_k10000_v3",
    "dict_sasb_categories",
    "gemma_sasb_categories",
    "claude_sasb_categories",
    "gpt_sasb_categories",
]
merged[col_order].to_csv(OUT_PATH, index=False)
print(f"\nSaved {len(merged):,} rows to:\n  {OUT_PATH}")

# ── 6. Summary table ─────────────────────────────────────────────────────────
print("\nGenerating summary table ...")

df = pd.read_csv(OUT_PATH)
SUMMARY_COLS = ("dict_sasb_categories", "gemma_sasb_categories",
                "gemma_desc_sasb_categories", "gemma_combined_sasb_categories")

for col in SUMMARY_COLS:
    if col in df.columns:
        df[col] = df[col].apply(parse_list_col)
    else:
        df[col] = [[]] * len(df)

all_categories = sorted({
    cat
    for col in SUMMARY_COLS
    for cats in df[col]
    for cat in cats
})

rows = []
for cat in all_categories:
    dict_hit     = df["dict_sasb_categories"].apply(lambda x: cat in x)
    role_hit     = df["gemma_sasb_categories"].apply(lambda x: cat in x)
    desc_hit     = df["gemma_desc_sasb_categories"].apply(lambda x: cat in x)
    combined_hit = df["gemma_combined_sasb_categories"].apply(lambda x: cat in x)
    rows.append({
        "sasb_category":        cat,
        "dict_n":               dict_hit.sum(),
        "dict_rate_%":          round(dict_hit.mean() * 100, 3),
        "gemma_role_n":         role_hit.sum(),
        "gemma_role_rate_%":    round(role_hit.mean() * 100, 3),
        "gemma_desc_n":         desc_hit.sum(),
        "gemma_desc_rate_%":    round(desc_hit.mean() * 100, 3),
        "gemma_combined_n":     combined_hit.sum(),
        "gemma_combined_rate_%": round(combined_hit.mean() * 100, 3),
    })

dict_any     = df["dict_sasb_categories"].apply(lambda x: len(x) > 0)
role_any     = df["gemma_sasb_categories"].apply(lambda x: len(x) > 0)
desc_any     = df["gemma_desc_sasb_categories"].apply(lambda x: len(x) > 0)
combined_any = df["gemma_combined_sasb_categories"].apply(lambda x: len(x) > 0)
rows.append({
    "sasb_category":         "TOTAL (any category)",
    "dict_n":                dict_any.sum(),
    "dict_rate_%":           round(dict_any.mean() * 100, 3),
    "gemma_role_n":          role_any.sum(),
    "gemma_role_rate_%":     round(role_any.mean() * 100, 3),
    "gemma_desc_n":          desc_any.sum(),
    "gemma_desc_rate_%":     round(desc_any.mean() * 100, 3),
    "gemma_combined_n":      combined_any.sum(),
    "gemma_combined_rate_%": round(combined_any.mean() * 100, 3),
})

summary = pd.DataFrame(rows)
SUMMARY_PATH = BASE / "summary_hit_rate.csv"
summary.to_csv(SUMMARY_PATH, index=False)
print(summary.to_string(index=False))
print(f"\nSaved summary to:\n  {SUMMARY_PATH}")
