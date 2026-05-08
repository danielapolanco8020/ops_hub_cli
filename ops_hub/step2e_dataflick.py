import pandas as pd
from pathlib import Path

from config import OUT_STEP2, OUT_STEP1, DATAFLICK_DEFAULT_CHUNK
from utils.file_helpers import (
    get_files_by_cadence, read_excel, save_excel,
    prompt_int, prompt_yes_no, make_output_path,
    print_header, print_step, print_done, print_warn, print_error,
)

# ── Dataflick column mapping: source column → export column name ───────────────
DATAFLICK_COLUMN_MAP = {
    "ADDRESS":              "Property Address",
    "CITY":                 "Property City",
    "STATE":                "Property State",
    "ZIP":                  "Property Zip",
    "MAILING ADDRESS":      "Mailing Address",
    "MAILING CITY":         "Mailing City",
    "MAILING STATE":        "Mailing State",
    "MAILING ZIP":          "Mailing Zip",
    "PHONE NUMBER 1":       "Phone 1",
    "PHONE TYPE 1":         "Phone 1 Type",
    "PHONE NUMBER 2":       "Phone 2",
    "PHONE TYPE 2":         "Phone 2 Type",
    "PHONE NUMBER 3":       "Phone 3",
    "PHONE TYPE 3":         "Phone 3 Type",
    "PHONE NUMBER 4":       "Phone 4",
    "PHONE TYPE 4":         "Phone 4 Type",
}


def run():
    print_header("STEP 2E — DATAFLICK FORMAT")

    # Only applicable to SMS or Cold Calling
    print("\n  Dataflick format applies to SMS and Cold Calling files only.")
    print("    1. SMS")
    print("    2. Cold Calling")
    print("    3. Both")
    while True:
        choice = input("  Select (1-3): ").strip()
        if choice == "1":
            cadences = ["SMS"]
            break
        elif choice == "2":
            cadences = ["Cold Calling"]
            break
        elif choice == "3":
            cadences = ["SMS", "Cold Calling"]
            break
        else:
            print("  Enter 1, 2 or 3.")

    # Confirm chunk size
    print(f"\n  Default chunk size: {DATAFLICK_DEFAULT_CHUNK:,} rows")
    use_default = prompt_yes_no("  Use default chunk size?", default=True)
    chunk_size  = DATAFLICK_DEFAULT_CHUNK if use_default else prompt_int(
        "  Enter chunk size", DATAFLICK_DEFAULT_CHUNK, min_val=1000
    )

    for cadence in cadences:
        # Resolve per cadence
        input_dir = None
        for folder in [OUT_STEP2, OUT_STEP1]:
            if get_files_by_cadence(folder, cadence):
                input_dir = folder
                break
        if not input_dir:
            print_warn(f"  No {cadence} files found in any output folder.")
            continue
        files = get_files_by_cadence(input_dir, cadence)
        if not files:
            print_warn(f"  No {cadence} files found in {input_dir.name}/")
            continue

        print_step(f"Found {len(files)} {cadence} file(s)")

        for f in files:
            print_step(f"Processing: {f.name}")
            df = read_excel(f)
            if df is None:
                continue

            # Find available columns from the mapping
            available = {src: dst for src, dst in DATAFLICK_COLUMN_MAP.items() if src in df.columns}
            missing   = [src for src in DATAFLICK_COLUMN_MAP if src not in df.columns]

            if not available:
                print_warn(f"  No matching Dataflick columns found — skipping.")
                continue
            if missing:
                print_warn(f"  Missing columns (will be skipped): {', '.join(missing)}")

            # Build export with renamed columns
            subset = df[list(available.keys())].copy()
            subset.rename(columns=available, inplace=True)

            if len(subset) > chunk_size:
                n_chunks = (len(subset) - 1) // chunk_size + 1
                print_step(f"  {len(subset):,} rows → {n_chunks} chunks of {chunk_size:,}")
                for i in range(n_chunks):
                    chunk    = subset.iloc[i * chunk_size : (i + 1) * chunk_size]
                    out_path = make_output_path(
                        OUT_STEP2, f.name,
                        prefix="dataflick_",
                        suffix=f"_chunk{i+1}"
                    )
                    save_excel(chunk, out_path)
                    print_done(f"  Chunk {i+1}: {len(chunk):,} rows → {out_path.name}")
            else:
                out_path = make_output_path(OUT_STEP2, f.name, prefix="dataflick_")
                save_excel(subset, out_path)
                print_done(f"  {len(subset):,} rows → {out_path.name}")

    print_done(f"Dataflick format complete → {OUT_STEP2}")