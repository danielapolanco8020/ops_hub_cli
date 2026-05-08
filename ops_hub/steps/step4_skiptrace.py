import pandas as pd
from pathlib import Path

from config import OUT_STEP3, OUT_STEP2, OUT_STEP1, OUT_STEP4, SKIPTRACE_EXPORT_COLUMNS
from utils.file_helpers import (
    get_files_by_cadence, read_excel, save_excel,
    make_output_path,
    print_header, print_step, print_done, print_warn, print_error,
)

# Columns used to identify duplicates (from source columns, before rename)
DEDUP_COLS = ["FOLIO", "ADDRESS", "ZIP"]


def run():
    print_header("STEP 4 — SKIPTRACE PRE-EXPORT")

    # Cadence selection — CC and SMS only
    print("\n  Select cadence to export for skiptrace:")
    print("    1. Cold Calling")
    print("    2. SMS")
    print("    3. Both")
    while True:
        choice = input("  Enter choice (1-3): ").strip()
        if choice == "1":
            cadences = ["Cold Calling"]
            break
        elif choice == "2":
            cadences = ["SMS"]
            break
        elif choice == "3":
            cadences = ["Cold Calling", "SMS"]
            break
        else:
            print("  Enter 1, 2 or 3.")

    source_cols  = list(SKIPTRACE_EXPORT_COLUMNS.keys())
    all_frames: list[pd.DataFrame] = []

    for cadence in cadences:
        # Resolve per cadence
        input_dir = None
        for folder in [OUT_STEP3, OUT_STEP2, OUT_STEP1]:
            if get_files_by_cadence(folder, cadence):
                input_dir = folder
                break

        if not input_dir:
            print_warn(f"  No files found for cadence '{cadence}' in any output folder.")
            continue

        files = get_files_by_cadence(input_dir, cadence)
        print_step(f"Found {len(files)} {cadence} file(s) in {input_dir.name}/")

        for f in files:
            print_step(f"Reading: {f.name}  [{cadence}]")
            df = read_excel(f)
            if df is None:
                continue

            # Check required source columns exist
            missing = [c for c in source_cols if c not in df.columns]
            if missing:
                print_warn(f"  Missing columns: {', '.join(missing)} — skipping.")
                continue

            all_frames.append(df[source_cols].copy())
            print_done(f"  {len(df):,} rows loaded.")

    if not all_frames:
        print_error("No valid files found across selected cadences. Nothing exported.")
        return

    # ── Merge all frames ───────────────────────────────────────────────────────
    merged = pd.concat(all_frames, ignore_index=True)
    print_step(f"Total rows after merge: {len(merged):,}")

    # ── Drop duplicates on FOLIO + ADDRESS + ZIP ───────────────────────────────
    dedup_present = [c for c in DEDUP_COLS if c in merged.columns]
    before        = len(merged)
    merged        = merged.drop_duplicates(subset=dedup_present, keep="first")
    removed       = before - len(merged)
    print_done(f"Duplicates removed: {removed:,}  ({len(merged):,} rows remaining)")

    # ── Rename columns to skiptrace platform format ────────────────────────────
    merged.rename(columns=SKIPTRACE_EXPORT_COLUMNS, inplace=True)

    # ── Save single merged export file ────────────────────────────────────────
    out_path = OUT_STEP4 / "skiptrace_upload.xlsx"
    save_excel(merged, out_path)
    print_done(f"Export saved → {out_path.name}  ({len(merged):,} rows)")

    print(f"\n  Next steps:")
    print(f"  1. Upload 'skiptrace_upload.xlsx' from 'output/step4_skiptrace/export/'")
    print(f"  2. Once results come back, place them in 'merge/skiptrace/'")
    print(f"  3. Post-merge functionality can be added in a future step")