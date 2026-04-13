import os
import sys
import json
import requests
import pandas as pd
from pathlib import Path

from config import (
    OUT_STEP1, OUT_STEP2, OUT_ZESTIMATE_EX, OUT_ZESTIMATE_MG, MERGE_ZESTIMATE,
    ZESTIMATE_EXPORT_COLUMNS, ZESTIMATE_MERGE_KEYS,
    ZESTIMATE_VALUE_COL, ZESTIMATE_OUTPUT_COL,
)
from utils.file_helpers import (
    get_excel_files, get_files_by_cadence, read_excel, save_excel, prompt_file_selection,
    print_header, print_step, print_done, print_warn, print_error,
    make_output_path,
)


# ── Sub-step 1: Pre-upload Export ──────────────────────────────────────────────

def run_export():
    print_header("ZESTIMATE — PRE-UPLOAD EXPORT")

    # Use step2 output if available, fall back to step1
    input_dir = OUT_STEP2 if list(get_files_by_cadence(OUT_STEP2, "Direct Mail")) else OUT_STEP1

    files = get_files_by_cadence(input_dir, "Direct Mail")
    if not files:
        print_error(f"No Direct Mail files found in {input_dir.name}/")
        return

    print_step(f"Found {len(files)} Direct Mail file(s) in {input_dir.name}/")

    for f in files:
        print_step(f"Processing: {f.name}")
        df = read_excel(f)
        if df is None:
            continue

        missing = [c for c in ZESTIMATE_EXPORT_COLUMNS if c not in df.columns]
        if missing:
            print_warn(f"  Missing columns: {', '.join(missing)} — skipping.")
            continue

        export_df = df[ZESTIMATE_EXPORT_COLUMNS].copy()
        out_path  = make_output_path(OUT_ZESTIMATE_EX, f.name, prefix="zestimate_upload_")
        save_excel(export_df, out_path)
        print_done(f"  Export saved → {out_path.name}  ({len(export_df):,} rows)")

    print_done(f"\nUpload files ready in: {OUT_ZESTIMATE_EX}")
    print(f"\n  Next steps:")
    print(f"  1. Upload the file(s) from '{OUT_ZESTIMATE_EX.name}/' to your WSE provider")
    print(f"  2. Wait for the job to complete and get your Job ID")
    print(f"  3. Place the results file in '{MERGE_ZESTIMATE.name}/'")
    print(f"  4. Run the Zestimate Merge step")


# ── Sub-step 2: Post-download Merge ───────────────────────────────────────────

def run_merge():
    print_header("ZESTIMATE — POST-DOWNLOAD MERGE")

    api_key = input("\n  Enter WSE API Key: ").strip()
    if not api_key:
        print_error("API key cannot be empty.")
        return

    job_id = input("  Enter WSE Job ID: ").strip()
    if not job_id:
        print_error("Job ID cannot be empty.")
        return

    # Use step2 output if available, fall back to step1
    input_dir = OUT_STEP2 if list(get_files_by_cadence(OUT_STEP2, "Direct Mail")) else OUT_STEP1

    dm_files = get_files_by_cadence(input_dir, "Direct Mail")
    if not dm_files:
        print_error(f"No Direct Mail files found in {input_dir.name}/")
        return
    print_step("Select the Direct Mail file to merge results into:")
    original_file = prompt_file_selection(input_dir, "Direct Mail file")
    if original_file is None:
        return

    # ── Download from WSE ──────────────────────────────────────────────────────
    print_step(f"Downloading results for Job ID: {job_id}")

    headers = {
        "Content-Type":  "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    params = {"id": job_id, "output_type": "csv"}

    try:
        resp = requests.get(
            "https://wse.icebergdata.io/public-api/downloadOutput",
            headers=headers,
            params=params,
            timeout=60,
        )
    except requests.RequestException as e:
        print_error(f"Request failed: {e}")
        return

    data_dir     = MERGE_ZESTIMATE
    output_files = []

    if resp.status_code == 202:
        print_warn("Job not ready yet. Please wait and try again.")
        return
    elif resp.status_code != 200:
        print_error(f"Download failed — HTTP {resp.status_code}")
        return

    content_type = resp.headers.get("Content-Type", "")
    if "text/csv" in content_type:
        csv_path = data_dir / f"{job_id}.csv"
        csv_path.write_bytes(resp.content)
        output_files.append(csv_path)
        print_done(f"Downloaded CSV → {csv_path.name}")
    else:
        # Signed URLs (multi-part)
        signed_path = data_dir / f"{job_id}_signed_urls.json"
        signed_path.write_bytes(resp.content)
        signed_urls = json.loads(resp.content)
        for i, url in enumerate(signed_urls):
            part_path = data_dir / f"{job_id}-{i}.csv"
            if not part_path.exists():
                r2 = requests.get(url, timeout=60)
                part_path.write_bytes(r2.content)
            output_files.append(part_path)
        print_done(f"Downloaded {len(output_files)} part(s)")

    # ── Combine output files ───────────────────────────────────────────────────
    dfs      = [pd.read_csv(p) for p in output_files]
    wse_df   = pd.concat(dfs, ignore_index=True)
    combined = data_dir / f"{job_id}_combined.csv"
    wse_df.to_csv(combined, index=False)
    print_done(f"Combined results: {len(wse_df):,} rows")

    # ── Merge with original ────────────────────────────────────────────────────
    print_step(f"Merging with: {original_file.name}")
    original_df = read_excel(original_file)
    if original_df is None:
        return

    missing_keys = [k for k in ZESTIMATE_MERGE_KEYS if k not in original_df.columns]
    if missing_keys:
        print_error(f"Original file missing merge keys: {', '.join(missing_keys)}")
        return

    if ZESTIMATE_VALUE_COL not in wse_df.columns:
        print_error(f"WSE results missing '{ZESTIMATE_VALUE_COL}' column.")
        print(f"  Available columns: {', '.join(wse_df.columns.tolist())}")
        return

    merged = pd.merge(original_df, wse_df, on=ZESTIMATE_MERGE_KEYS, how="left")

    if "ZIP_y" in merged.columns:
        merged.drop(columns=["ZIP_y"], inplace=True)
    if "ZIP_x" in merged.columns:
        merged.rename(columns={"ZIP_x": "ZIP"}, inplace=True)

    merged[ZESTIMATE_OUTPUT_COL] = merged[ZESTIMATE_VALUE_COL]
    print_done(f"Created '{ZESTIMATE_OUTPUT_COL}' column from '{ZESTIMATE_VALUE_COL}'")

    out_path = make_output_path(OUT_ZESTIMATE_MG, original_file.name, prefix="zestimate_merged_")
    save_excel(merged, out_path)
    print_done(f"Merged file saved → {out_path.name}  ({len(merged):,} rows)")


# ── Entry Point ────────────────────────────────────────────────────────────────

def run():
    print_header("ZESTIMATE TOOL")
    print("\n  1. Pre-upload Export  (generate WSE upload file)")
    print("  2. Post-download Merge (download results & merge)")
    while True:
        choice = input("\n  Select option (1 or 2): ").strip()
        if choice == "1":
            run_export()
            break
        elif choice == "2":
            run_merge()
            break
        else:
            print("  Enter 1 or 2.")