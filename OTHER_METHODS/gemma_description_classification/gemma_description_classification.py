#!/usr/bin/env python3

# Script for classifying job descriptions into SASB categories using Ollama
# Uses asyncio for parallel processing to maximize GPU utilization

import os
import re
import json
import asyncio
import random
import itertools
import signal
import subprocess
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Any
from urllib.parse import urlparse

import pandas as pd
import aiohttp
from aiohttp import ClientTimeout

import argparse

parser = argparse.ArgumentParser(
    description="Classify job descriptions into SASB categories with Ollama"
)
parser.add_argument("--shard-id", type=int, default=0)
parser.add_argument("--num-shards", type=int, default=6)
parser.add_argument(
    "--input-csv", type=str,
    default="/gpfs/home/fl488/process_RL_sasb/gemma_role_classification/comparison_sasb.csv",
)
parser.add_argument(
    "--output-csv", type=str,
    default="/gpfs/home/fl488/process_RL_sasb/gemma_description_classification/gemma_description_classification_output.csv",
)
parser.add_argument(
    "--max-concurrent", type=int, default=10,
    help="Maximum number of concurrent requests to Ollama",
)
args = parser.parse_args()

# Track Ollama server subprocesses or external PIDs by GPU index
server_processes: Dict[int, subprocess.Popen | int] = {}

# Detect externally launched Ollama servers (via SLURM) and populate server_processes
for idx, port in enumerate((11434, 11435, 11436)):
    try:
        completed = subprocess.run(
            ["lsof", "-ti", f":{port}"],
            capture_output=True, text=True, check=True
        )
        pids = completed.stdout.strip().split()
        if pids:
            server_processes[idx] = int(pids[0])
            print(f"[Startup] Found external Ollama PID {pids[0]} on port {port}", flush=True)
    except Exception as e:
        print(f"[Startup] No external Ollama detected on port {port}: {e}", flush=True)


MAX_CONCURRENT_REQUESTS = args.max_concurrent


def detect_local_gpus() -> int:
    try:
        out = subprocess.run(["nvidia-smi", "-L"], capture_output=True, text=True).stdout
        lines = out.strip().splitlines()
        if lines:
            return len(lines)
    except Exception:
        pass
    cvis = os.environ.get("CUDA_VISIBLE_DEVICES")
    return len(cvis.split(",")) if cvis else 1


num_local_gpus = detect_local_gpus()
base_port = 11434
OLLAMA_API_URLS = [
    f"http://127.0.0.1:{base_port + i}/api/generate" for i in range(num_local_gpus)
]
if not OLLAMA_API_URLS:
    OLLAMA_API_URLS = ["http://127.0.0.1:11434/api/generate"]

gpu_cycle = itertools.cycle(OLLAMA_API_URLS)
MODEL = "gemma3:27b"

CLIENT_TIMEOUT = ClientTimeout(total=600)

# Load the prompt template from the same directory as this script
_HERE = os.path.dirname(os.path.abspath(__file__))
PROMPT_TEMPLATE_PATH = os.path.join(_HERE, "gemma_description_classification_prompt.md")
with open(PROMPT_TEMPLATE_PATH) as _f:
    PROMPT_TEMPLATE = _f.read()

# Valid SASB category names as defined in the prompt
VALID_SASB_CATEGORIES = {
    "GHG_Emissions",
    "Air_Quality",
    "Energy_Management",
    "Water_&_Wastewater_Management",
    "Waste_&_Hazardous_Materials_Management",
    "Ecological_Impacts",
    "Human_Rights_&_Community_Relations",
    "Customer_Privacy",
    "Data_Security",
    "Access_&_Affordability",
    "Product_Quality_&_Safety",
    "Customer_Welfare",
    "Selling_Practices_&_Product_Labeling",
    "Labor_Practices",
    "Employee_Health_&_Safety",
    "Employee_Engagement,_Diversity_&_Inclusion",
    "Product_Design_&_Lifecycle_Management",
    "Business_Model_Resilience",
    "Supply_Chain_Management",
    "Materials_Sourcing_&_Efficiency",
    "Physical_Impacts_of_Climate_Change",
    "Business_Ethics",
    "Competitive_Behavior",
    "Management_of_the_Legal_&_Regulatory_Environment",
    "Critical_Incident_Risk_Management",
    "Systemic_Risk_Management",
}


@dataclass
class DescriptionBatch:
    batch_id: int
    positions: List[int]
    rows: List[Dict[str, str]]  # each: {"position_id": ..., "description": ...}


restart_lock = asyncio.Lock()
global_cooldown_future: Optional[asyncio.Task] = None
global_cooldown_lock = asyncio.Lock()


def build_prompt(row: Dict[str, str]) -> str:
    """Fill the prompt template for a single job posting."""
    return PROMPT_TEMPLATE.replace("{job_description}", row.get("description", "").strip())


def parse_json_response(response: str) -> Optional[Dict]:
    """Extract and parse a JSON object from the LLM response, handling code blocks."""
    cleaned = re.sub(r"```(?:json)?\s*", "", response).strip().rstrip("`").strip()

    match = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        return None


def clean_result(result: Any) -> Optional[Dict]:
    """
    Extract sasb_categories and confidence from a parsed result dict.
    Invalid SASB categories are silently dropped (not a retry trigger).
    Returns None if the result is fundamentally unparseable.
    """
    if not isinstance(result, dict):
        return None
    cats = result.get("sasb_categories", [])
    if not isinstance(cats, list):
        cats = []
    valid_cats = [c for c in cats if c in VALID_SASB_CATEGORIES]
    if len(valid_cats) < len(cats):
        dropped = [c for c in cats if c not in VALID_SASB_CATEGORIES]
        print(f"[WARN] Dropped unknown SASB categories: {dropped}", flush=True)
    return {"sasb_categories": valid_cats}


async def call_ollama_api(
    session: aiohttp.ClientSession,
    batch: DescriptionBatch,
) -> Tuple[DescriptionBatch, str, str]:
    """Send a single job posting to Ollama and return the raw response."""
    prompt = build_prompt(batch.rows[0])

    payload = {
        "model": MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {"seed": 42, "temperature": 0.1, "num_ctx": 8192},
    }

    api_url = next(gpu_cycle)
    try:
        async with session.post(api_url, json=payload) as resp:
            if resp.status == 200:
                data = await resp.json()
                return batch, data.get("response", ""), api_url
            else:
                txt = await resp.text()
                return batch, f"ERROR: status {resp.status}: {txt}", api_url
    except Exception as e:
        return batch, f"ERROR: {e}", api_url


async def restart_ollama_server_async(api_url: str):
    try:
        parsed = urlparse(api_url)
        port = parsed.port
        if port == 11434:
            gpu_index = 0
        elif port == 11435:
            gpu_index = 1
        elif port == 11436:
            gpu_index = 2
        else:
            print(f"[Restart] Unknown port {port} in URL {api_url}", flush=True)
            return

        print(f"[Restart] Initiating restart for GPU {gpu_index} (port {port})...", flush=True)

        prev = server_processes.get(gpu_index)
        if isinstance(prev, (int, subprocess.Popen)):
            pid = prev if isinstance(prev, int) else prev.pid
            print(f"[Restart] Terminating PID {pid}", flush=True)
            try:
                os.kill(pid, signal.SIGKILL)
            except Exception as e:
                print(f"[Restart] Warning terminating PID {pid}: {e}", flush=True)
        else:
            proc = await asyncio.create_subprocess_exec(
                "lsof", "-ti", f":{port}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            out, _ = await proc.communicate()
            for pid_str in out.decode().split():
                if pid_str.isdigit() and int(pid_str) != os.getpid():
                    try:
                        os.kill(int(pid_str), signal.SIGKILL)
                        print(f"[Restart] Killed PID {pid_str} on port {port}", flush=True)
                    except Exception as e:
                        print(f"[Restart] Warning killing PID {pid_str}: {e}", flush=True)

        await asyncio.sleep(0.5)

        env = os.environ.copy()
        env["CUDA_VISIBLE_DEVICES"] = str(gpu_index)
        env["OLLAMA_HOST"] = f"http://127.0.0.1:{port}"
        proc = await asyncio.create_subprocess_exec(
            "ollama", "start",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        server_processes[gpu_index] = proc.pid
        print(f"[Restart] Ollama daemon started with PID {proc.pid}", flush=True)

        for attempt in range(1, 31):
            try:
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                writer.close()
                await writer.wait_closed()
                print(f"[Restart] Port {port} open (after {attempt}s)", flush=True)
                return True
            except Exception:
                pass
            await asyncio.sleep(1)

        print(f"[Restart] ERROR: port {port} never opened after 30s", flush=True)
        return False
    except Exception as e:
        print(f"[Restart] ERROR: {e}", flush=True)


async def call_ollama_api_with_retry(
    session: aiohttp.ClientSession,
    batch: DescriptionBatch,
    max_retries: int = 5,
    initial_delay: int = 5,
) -> Tuple[DescriptionBatch, Dict]:
    """Call Ollama with retries. Returns the batch paired with a cleaned result dict."""

    connection_error_indicators = [
        "server disconnected",
        "server busy",
        "connection reset by peer",
        "eof",
        "connect: connection refused",
        "cannot connect to host",
    ]

    for attempt in range(1, max_retries + 1):
        batch_out, res, used_url = await call_ollama_api(session, batch)
        low = res.lower()

        if "signal: killed" in low or "runner process no longer running" in low:
            print(f"[Retry {attempt}] Runner crash for row {batch.positions[0]}; retrying...", flush=True)
            await asyncio.sleep(10)
            continue

        if any(err in low for err in connection_error_indicators):
            print(f"[Retry {attempt}] Connection error for row {batch.positions[0]}: {res[:200]}", flush=True)
            async with restart_lock:
                try:
                    await restart_ollama_server_async(used_url)
                except Exception as e:
                    print(f"[Retry {attempt}] Restart error: {e}", flush=True)
            base = initial_delay * (2 ** (attempt - 1))
            delay = min(base, 30) * (1 + random.uniform(0.1, 0.2))
            async with global_cooldown_lock:
                global global_cooldown_future
                if global_cooldown_future is None or global_cooldown_future.done():
                    global_cooldown_future = asyncio.create_task(asyncio.sleep(delay))
                    print(f"Global cooldown: {delay:.1f}s", flush=True)
            await global_cooldown_future
            continue

        if re.search(r"error: status 5\d\d", low):
            print(f"[Retry {attempt}] HTTP error for row {batch.positions[0]}: {res[:200]}", flush=True)
            await asyncio.sleep(initial_delay * (1 + random.uniform(0.1, 0.2)))
            continue

        parsed = parse_json_response(res)
        if parsed is None:
            print(
                f"[Retry {attempt}] Non-JSON response for row {batch.positions[0]}: {res[:200]!r}",
                flush=True,
            )
            await asyncio.sleep(initial_delay * (1 + random.uniform(0.1, 0.2)))
            continue

        result = clean_result(parsed)
        if result is None:
            print(
                f"[Retry {attempt}] Unparseable result for row {batch.positions[0]}; retrying...",
                flush=True,
            )
            await asyncio.sleep(initial_delay * (1 + random.uniform(0.1, 0.2)))
            continue

        return batch, result

    print(
        f"[ERROR] Max retries reached for row {batch.positions[0]}. Returning empty default.",
        flush=True,
    )
    return batch, {"sasb_categories": []}


async def classify_batches(
    batches: List[DescriptionBatch],
) -> Dict[int, Dict]:
    """Classify all rows concurrently. Returns position → result mapping."""
    results: Dict[int, Dict] = {}
    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def worker(b: DescriptionBatch):
        async with sem:
            return await call_ollama_api_with_retry(session, b)

    async with aiohttp.ClientSession(timeout=CLIENT_TIMEOUT) as session:
        tasks = [worker(b) for b in batches]
        for coro in asyncio.as_completed(tasks):
            batch, result = await coro
            results[batch.positions[0]] = result
            print(f"Classified row {batch.positions[0]}: {result}", flush=True)

    return results


async def main_async():
    print("✅ SASB description classification main started", flush=True)

    if not os.path.isfile(args.input_csv):
        print(f"Input CSV not found: {args.input_csv}", flush=True)
        return

    print(f"Reading input CSV: {args.input_csv}", flush=True)
    df = pd.read_csv(args.input_csv)

    required_cols = {"position_id", "description"}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"Input CSV must contain columns: {required_cols}")

    df["description"] = df["description"].fillna("").astype(str)
    df["position_id"] = df["position_id"].astype(str)

    total_rows = len(df)
    print(f"Total rows in CSV: {total_rows}", flush=True)

    all_positions = list(range(total_rows))
    shard_positions = all_positions[args.shard_id :: args.num_shards]
    print(
        f"Processing shard {args.shard_id}/{args.num_shards - 1}: {len(shard_positions)} rows",
        flush=True,
    )

    batches: List[DescriptionBatch] = []
    skipped: Dict[int, Dict] = {}
    for pos in shard_positions:
        desc = df.iloc[pos]["description"].strip()
        if not desc:
            skipped[pos] = {"sasb_categories": []}
            continue
        row = {
            "position_id": df.iloc[pos]["position_id"],
            "description": desc,
        }
        batches.append(DescriptionBatch(batch_id=len(batches), positions=[pos], rows=[row]))

    print(f"Created {len(batches)} single-row jobs ({len(skipped)} skipped — empty description)", flush=True)

    results_map = await classify_batches(batches)
    results_map.update(skipped)

    records = []
    for pos in sorted(results_map):
        row = df.iloc[pos]
        result = results_map[pos]
        records.append(
            {
                "row_position": pos,
                "position_id": row["position_id"],
                "sasb_categories": json.dumps(result["sasb_categories"]),
            }
        )

    out_df = pd.DataFrame(records)

    base_out = args.output_csv
    base, ext = os.path.splitext(base_out)
    if ext == "":
        ext = ".csv"
    out_path = (
        f"{base}_shard{args.shard_id}{ext}" if args.num_shards > 1 else f"{base}{ext}"
    )

    out_df.to_csv(out_path, index=False)
    print(f"✅ Wrote SASB classifications to: {out_path}", flush=True)


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
