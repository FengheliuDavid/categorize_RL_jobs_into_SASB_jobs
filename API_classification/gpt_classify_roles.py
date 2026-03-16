"""
Classify role_combinations.csv into SASB categories using GPT-4o.
Processes 10 roles per API call. Saves results to output_sasb_gpt.csv.
Automatically resumes from checkpoint if interrupted.

Usage:
    set OPENAI_API_KEY=your_key_here
    python gpt_classify_roles.py
"""

import json
import time
import openai
import pandas as pd
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
BASE        = Path("D:/fenghe/dropbox/Dropbox/fengheliu/temp/sasb_jobs")
INPUT_CSV   = BASE / "role_combinations.csv"
OUTPUT_CSV  = BASE / "output_sasb_gpt.csv"
PROMPT_FILE = Path(__file__).parent / "llm_classification_prompt.md"
BATCH_SIZE  = 10
MODEL       = "gpt-4o"

# ── Load system prompt (strip the "## Job Role" section at the end) ───────────
raw_prompt = PROMPT_FILE.read_text(encoding="utf-8")
system_prompt = raw_prompt.split("## Job Role")[0].strip()

# ── Load roles ────────────────────────────────────────────────────────────────
df = pd.read_csv(INPUT_CSV).fillna("")
total = len(df)
print(f"Total roles to classify: {total:,}")

# ── Resume from checkpoint ────────────────────────────────────────────────────
if OUTPUT_CSV.exists():
    done_df = pd.read_csv(OUTPUT_CSV)
    start_idx = len(done_df)
    print(f"Resuming from row {start_idx:,} ({start_idx/total:.1%} already done)")
else:
    done_df = pd.DataFrame(columns=[
        "row_position", "role_k1500_v3", "role_k5000_v3",
        "role_k10000_v3", "sasb_categories", "confidence",
    ])
    start_idx = 0

if start_idx >= total:
    print("All rows already classified.")
    exit()

# ── OpenAI client (reads OPENAI_API_KEY from environment) ─────────────────────
client = openai.OpenAI()

# ── Process batches ───────────────────────────────────────────────────────────
for batch_start in range(start_idx, total, BATCH_SIZE):
    batch = df.iloc[batch_start : batch_start + BATCH_SIZE]

    # Build numbered role list for the user message
    role_lines = []
    for i, (_, row) in enumerate(batch.iterrows(), 1):
        role_lines.append(
            f"{i}. role_k1500={row['role_k1500_v3']} | "
            f"role_k5000={row['role_k5000_v3']} | "
            f"role_k10000={row['role_k10000_v3']}"
        )

    user_msg = (
        "Classify each job role below into SASB categories.\n"
        "Return a JSON array with exactly one object per role, in the same order:\n"
        '[{"sasb_categories": [...], "confidence": "high|medium|low"}, ...]\n\n'
        + "\n".join(role_lines)
    )

    # Call GPT-4o with retries
    results = None
    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_msg},
                ],
                max_tokens=1024,
                temperature=0,
            )
            raw = response.choices[0].message.content.strip()
            # Strip markdown code fences if present
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            results = json.loads(raw)
            break
        except Exception as e:
            print(f"  Attempt {attempt+1} failed at row {batch_start}: {e}")
            time.sleep(2 ** attempt)

    if results is None:
        print(f"  Skipping batch {batch_start} after 3 failures — filling with empty")
        results = [{"sasb_categories": [], "confidence": "error"}] * len(batch)

    # Build rows and append to checkpoint file
    rows = []
    for i, (_, row) in enumerate(batch.iterrows()):
        r = results[i] if i < len(results) else {"sasb_categories": [], "confidence": "error"}
        rows.append({
            "row_position":   batch_start + i,
            "role_k1500_v3":  row["role_k1500_v3"],
            "role_k5000_v3":  row["role_k5000_v3"],
            "role_k10000_v3": row["role_k10000_v3"],
            "sasb_categories": r.get("sasb_categories", []),
            "confidence":      r.get("confidence", "error"),
        })

    done_df = pd.concat([done_df, pd.DataFrame(rows)], ignore_index=True)
    done_df.to_csv(OUTPUT_CSV, index=False)

    print(f"  [{batch_start + len(batch):,}/{total:,}]  batch done")
    time.sleep(0.3)

print(f"\nDone. Saved {total:,} rows to:\n  {OUTPUT_CSV}")
