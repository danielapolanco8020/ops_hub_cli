import pandas as pd
from pathlib import Path

from config import OUT_STEP1, OUT_STEP2
from utils.file_helpers import (
    get_excel_files, read_excel, save_excel,
    prompt_yes_no, print_header, print_step, print_done, print_warn, print_error,
    make_output_path,
)
from utils.name_helpers import clean_and_split_name, needs_name_split


def run():
    print_header("STEP 2G — NAME CLEANER & SPLITTER")

    input_dir = OUT_STEP2 if list(OUT_STEP2.glob("*.xlsx")) else OUT_STEP1
    files     = get_excel_files(input_dir)

    if not files:
        print_error(f"No Excel files found in {input_dir.name}/")
        return

    print_step(f"Found {len(files)} file(s) in {input_dir.name}/")

    for f in files:
        print_step(f"Processing: {f.name}")
        df = read_excel(f)
        if df is None:
            continue

        if "OWNER FULL NAME" not in df.columns:
            print_warn(f"  No 'OWNER FULL NAME' column — skipping.")
            continue

        if "OWNER FIRST NAME" not in df.columns:
            df["OWNER FIRST NAME"] = None
        if "OWNER LAST NAME" not in df.columns:
            df["OWNER LAST NAME"] = None

        needs_update = df.apply(needs_name_split, axis=1)
        rows_to_fix  = df[needs_update]

        if rows_to_fix.empty:
            print_done(f"  All rows already have first/last names — nothing to update.")
        else:
            print_step(f"  {len(rows_to_fix):,} rows with empty first/last name found.")
            print_step(f"  Cleaning and correcting name order...")

            cleaned = rows_to_fix["OWNER FULL NAME"].apply(clean_and_split_name)
            df.loc[needs_update, "OWNER FIRST NAME"] = cleaned.apply(lambda x: x[0])
            df.loc[needs_update, "OWNER LAST NAME"]  = cleaned.apply(lambda x: x[1])
            print_done(f"  Names cleaned and order corrected.")

        do_split = prompt_yes_no(
            "  Write split names into OWNER FIRST NAME / OWNER LAST NAME columns?",
            default=True,
        )

        if not do_split:
            print_warn(f"  Split skipped — cleaned names not written to columns.")
            continue

        out_path = make_output_path(OUT_STEP2, f.name, prefix="named_")
        save_excel(df, out_path)
        print_done(f"  Saved → {out_path.name}")

    print_done(f"Name splitter complete → {OUT_STEP2}")
