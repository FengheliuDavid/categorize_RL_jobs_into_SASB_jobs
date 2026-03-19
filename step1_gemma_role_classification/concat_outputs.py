import pandas as pd
import glob
import os

WORKDIR = os.path.dirname(os.path.abspath(__file__))
shard_files = sorted(glob.glob(os.path.join(WORKDIR, "gemma_role_classification_output_shard*.csv")))

if not shard_files:
    raise FileNotFoundError("No output_sasb_shard*.csv files found.")

df = pd.concat([pd.read_csv(f) for f in shard_files], ignore_index=True)
df = df.sort_values("row_position").reset_index(drop=True)

out_path = os.path.join(WORKDIR, "gemma_role_classification_output.csv")
df.to_csv(out_path, index=False)
print(f"Wrote {len(df)} rows to {out_path}")
