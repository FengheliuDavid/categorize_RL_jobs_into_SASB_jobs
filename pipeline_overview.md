# SASB Jobs Classification Pipeline

## Big Picture

This pipeline classifies every job position in the Revelio Labs database into one or more of the **26 SASB ESG categories** and aggregates them into monthly company-level hiring metrics.

The core idea: rather than classifying hundreds of millions of individual job postings (expensive), classify the ~10,000 unique **role labels** (`role_k10000_v3`) once using an LLM, then look up every position by its role. The final output is a balanced panel of **net new ESG hires** per company per month, normalized as a share of total net hiring.

| Dimension | Detail |
| --- | --- |
| Data source | Revelio Labs (`temp_processed_global_position`, ClickHouse) |
| Role taxonomy | `role_k10000_v3` — ~10,000 canonical role labels |
| SASB categories | 26 (Environmental, Social, Governance) |
| Sample window | 2007–2025 |
| Output granularity | company (`rcid`) × month × 26 SASB columns |
| Key metric | Net inflow = new hires − separations, as share of total net hiring |

## Step 1: Role Classification

The LLM classifies **role title triples** — not individual job postings — into SASB categories. Each unique combination of (`role_k1500_v3`, `role_k5000_v3`, `role_k10000_v3`) is sent once to **Gemma 3 27B** running via Ollama on a SLURM HPC cluster.

The prompt enforces a strict rule: a category qualifies only if the ESG issue is *the reason the role exists*. For example, an EHS Manager qualifies for `Employee_Health_&_Safety`; a generic Customer Service rep does not qualify for `Customer_Privacy`.

**Infrastructure:** async requests across 3 Ollama servers per GPU node, sharded across up to 18 GPUs; shard outputs merged by `concat_outputs.py`.

| | |
| --- | --- |
| Input | `role_combinations.csv` — unique role triples |
| Model | Gemma 3 27B, temperature 0.1 |
| Output | `gemma_role_classification_output.csv` — each triple → `sasb_categories[]` + confidence |
| Script | `step1_gemma_role_classification/gemma_role_classification.py` |

## Steps 2 & 3: Description Fallback → Role-to-SASB Mapping

Step 1 leaves ~7,591 of 10,000 role labels unclassified (titles too ambiguous). Steps 2 and 3 rescue a subset using job description text, then produce the single mapping table consumed by Step 4.

**Step 2 — description classification:** A stratified sample of ~**1,000 job postings per `role_k10000_v3`** (~10 million total — hence `10m` in the filename) is classified by Gemma using the full description text. The prompt is more restrictive than Step 1: a category qualifies only if the description explicitly states managing that ESG area as a core function. `MIN_N = 10` is the minimum number of sampled descriptions a role must have to be eligible for assignment (not the per-role sample target).

**Step 3 — combine (notebook `assign_unmatched_roles.ipynb`):** Fully automated, no manual labelling.

1. Take Step 1 results, group by `role_k10000_v3`, union all SASB categories → **2,411 roles mapped**
2. For each of the remaining 7,591 unmatched roles, compute the fraction of its ~1,000 sampled descriptions that were tagged with each SASB category. Assign the category if ≥ `MIN_RATE` of descriptions agree → **97 more roles mapped**
3. Save combined mapping as `role_to_sasb_mapping_{threshold}.csv`

| | |
| --- | --- |
| Threshold used (`MIN_RATE`) | 0.3 (≥ 30% of descriptions must agree) |
| Roles with SASB mapping | 2,508 of 10,000 (25%) |
| Source breakdown | 2,411 from role titles; 97 from description fallback |
| Output | `role_to_sasb_mapping_10m_0.3.csv` — one row per `role_k10000_v3` |

## Step 4: Monthly Aggregation

Two scripts run in sequence.

**Step 4a (`step1_pull_sasb_jobs_by_role.py`)** — queries ClickHouse:

1. Uploads the 2,508 mapped role labels to a temporary in-memory table
2. For each year 2007–2025: pulls all positions with a mapped role, partitioned by `startdate` → inflow parquets
3. Same again partitioned by `enddate` → outflow parquets (captures positions that started before 2007 but ended within the window)
4. Pulls total outflow for **all** positions (denominator) → `all_outflow_jobs_monthly.csv`
5. Pulls total gross inflow for **all** positions (denominator) → `all_new_jobs_monthly.csv` (pre-existing)

**Step 4b (`step2_aggregate_to_monthly.py`)** — aggregates in pandas:

1. Joins inflow positions with the role mapping, explodes `sasb_categories` (one position → one row per category)
2. Aggregates to `rcid × month × category`, sums weighted counts → inflow wide table (26 SASB columns)
3. Repeats for outflow positions
4. Computes **net = inflow − outflow** per cell
5. Builds a **balanced panel**: all RCIDs × all months 2007-01–2025-12, filling gaps with 0
6. Computes **net share** = each SASB net count ÷ total net jobs (inflow − outflow across all roles)

| | |
| --- | --- |
| Input | Inflow/outflow parquets + role mapping + denominator CSVs |
| Net can be negative | Yes — more separations than new hires in that category that month |
| NaN shares | When total net denominator = 0 (not filled with 0) |
| Scripts | `step4_create_monthly_data/step1_pull_sasb_jobs_by_role.py`, `step2_aggregate_to_monthly.py` |

## Current Outputs & Notes

**Output files** (in `cleaned_data/role_based_approach_net_inflow/`):

| File | Contents |
| --- | --- |
| `revelio_sasb_monthly_net_new_jobs_role_based_10m_0.3.parquet` | Net counts: `rcid × month × 26 SASB columns` (float32, can be negative) |
| `revelio_sasb_monthly_net_share_new_jobs_role_based_10m_0.3.parquet` | Same + `pct_<category>` columns (net SASB ÷ total net, NaN when denominator = 0) |

**Threshold:** `10m_0.3` means the Step 3 description fallback required ≥ 30% of sampled descriptions to agree. The notebook also has runs at 0.5; the step4 scripts currently use 0.3.

**Stock pull (future work):** Step 4a now also pulls `all_stock_jobs_monthly.csv` — total weighted headcount per company per month (employees active on the first of the month). Not yet used in Step 4b; in place for a potential future switch from the current net-inflow denominator to a stock-based denominator.
