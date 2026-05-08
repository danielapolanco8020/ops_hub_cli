import pandas as pd
from pathlib import Path

from config import (
    OUT_STEP1, OUT_STEP2, OUT_ZESTIMATE_EX, OUT_ZESTIMATE_MG, MERGE_ZESTIMATE,
    ZESTIMATE_EXPORT_COLUMNS, ZESTIMATE_MERGE_KEYS,
    ZESTIMATE_VALUE_COL, ZESTIMATE_OUTPUT_COL,
)
from utils.file_helpers import (
    get_excel_files, get_files_by_cadence, read_excel, save_excel,
    prompt_file_selection, print_header, print_step, print_done,
    print_warn, print_error, make_output_path,
)


# ── Sub-step 1: Pre-upload Export ──────────────────────────────────────────────

def run_export():
    print_header("ZESTIMATE — PRE-UPLOAD EXPORT")

    # Use step2 DM files if available, fall back to step1
    input_dir = None
    for folder in [OUT_STEP2, OUT_STEP1]:
        if get_files_by_cadence(folder, "Direct Mail"):
            input_dir = folder
            break

    if not input_dir:
        print_error("No Direct Mail files found in any output folder.")
        return

    files = get_files_by_cadence(input_dir, "Direct Mail")
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

    print(f"\n  Next steps:")
    print(f"  1. Upload file(s) from 'output/zestimate/export/' to your WSE provider")
    print(f"  2. Once results come back, drop the file into 'merge/zestimate/'")
    print(f"  3. Run the Zestimate Merge step")


# ── Sub-step 2: Merge ──────────────────────────────────────────────────────────

def run_merge():
    print_header("ZESTIMATE — MERGE")

    # Check for results file in merge/zestimate/
    result_files = get_excel_files(MERGE_ZESTIMATE)
    csv_files    = list(MERGE_ZESTIMATE.glob("*.csv"))
    all_files    = result_files + [Path(f) for f in csv_files]

    if not all_files:
        print_error(f"No files found in merge/zestimate/")
        print(f"  Drop your WSE results file there and run again.")
        return

    # Let user pick results file if multiple
    if len(all_files) == 1:
        results_path = all_files[0]
        print_step(f"Results file: {results_path.name}")
    else:
        print_step("Multiple files found in merge/zestimate/ — select results file:")
        for i, f in enumerate(all_files, 1):
            print(f"    {i}. {f.name}")
        while True:
            try:
                idx = int(input("  Select file: ").strip()) - 1
                if 0 <= idx < len(all_files):
                    results_path = all_files[idx]
                    break
                print(f"  Enter a number between 1 and {len(all_files)}.")
            except ValueError:
                print("  Enter a valid number.")

    # Load results file
    try:
        if results_path.suffix.lower() == ".csv":
            wse_df = pd.read_csv(results_path)
        else:
            wse_df = pd.read_excel(results_path, engine="openpyxl")
    except Exception as e:
        print_error(f"Could not read results file: {e}")
        return

    print_done(f"Results loaded: {len(wse_df):,} rows")

    # Check zestimate column exists
    if ZESTIMATE_VALUE_COL not in wse_df.columns:
        print_error(f"Results file missing '{ZESTIMATE_VALUE_COL}' column.")
        print(f"  Available columns: {', '.join(wse_df.columns.tolist())}")
        return

    # Select original DM file to merge against — per cadence resolution
    input_dir = None
    for folder in [OUT_STEP2, OUT_STEP1]:
        if get_files_by_cadence(folder, "Direct Mail"):
            input_dir = folder
            break

    if not input_dir:
        print_error("No Direct Mail files found in any output folder.")
        return

    print_step("Select the Direct Mail file to merge results into:")
    original_file = prompt_file_selection(input_dir, "Direct Mail file")
    if original_file is None:
        return

    original_df = read_excel(original_file)
    if original_df is None:
        return

    # Check merge keys exist
    missing_keys = [k for k in ZESTIMATE_MERGE_KEYS if k not in original_df.columns]
    if missing_keys:
        print_error(f"Original file missing merge keys: {', '.join(missing_keys)}")
        return

    # Merge on FOLIO + ADDRESS + CITY + STATE
    print_step(f"Merging on: {', '.join(ZESTIMATE_MERGE_KEYS)}")
    merged = pd.merge(original_df, wse_df, on=ZESTIMATE_MERGE_KEYS, how="left")

    # Clean up duplicate ZIP columns
    if "ZIP_y" in merged.columns:
        merged.drop(columns=["ZIP_y"], inplace=True)
    if "ZIP_x" in merged.columns:
        merged.rename(columns={"ZIP_x": "ZIP"}, inplace=True)

    # Create STICKER PRICE column from zestimate value
    merged[ZESTIMATE_OUTPUT_COL] = merged[ZESTIMATE_VALUE_COL]
    matched = merged[ZESTIMATE_OUTPUT_COL].notna().sum()
    print_done(f"Created '{ZESTIMATE_OUTPUT_COL}' column — {matched:,} rows matched")

    out_path = make_output_path(OUT_ZESTIMATE_MG, original_file.name, prefix="zestimate_merged_")
    save_excel(merged, out_path)
    print_done(f"Merged file saved → {out_path.name}  ({len(merged):,} rows)")


# ── Entry Point ────────────────────────────────────────────────────────────────

def run():
    print_header("ZESTIMATE TOOL")
    print("\n  1. Pre-upload Export  (generate WSE upload file)")
    print("  2. Merge              (merge WSE results into DM file)")
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