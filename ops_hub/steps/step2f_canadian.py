import pandas as pd
from pathlib import Path

from config import OUT_STEP2, OUT_STEP1, CANADIAN_PROVINCES
from utils.file_helpers import (
    get_excel_files, get_files_by_cadence, read_excel, save_excel,
    find_column,
    print_header, print_step, print_done, print_warn, print_error,
)


def _flag_canadian(df: pd.DataFrame, state_col: str) -> pd.DataFrame:
    df["_MailingStateClean"] = df[state_col].astype(str).str.strip().str.upper()
    df["_IsCanada"]          = df["_MailingStateClean"].isin(CANADIAN_PROVINCES)
    return df


def run():
    print_header("STEP 2F — CANADIAN MAIL FILTER")

    # Collect files per cadence to avoid missing any
    from config import CADENCES
    all_files = []
    for cadence in CADENCES:
        for folder in [OUT_STEP2, OUT_STEP1]:
            found = get_files_by_cadence(folder, cadence)
            if found:
                all_files.extend(found)
                break
    # Deduplicate by path
    files = list({f.name: f for f in all_files}.values())

    if not files:
        print_error(f"No Excel files found in {input_dir.name}/")
        return

    print_step(f"Found {len(files)} file(s) in {input_dir.name}/")

    all_canadian: list[pd.DataFrame] = []

    for f in files:
        print_step(f"Processing: {f.name}")
        df = read_excel(f)
        if df is None:
            continue

        state_col = find_column(df, [
            "MailingState", "MAILING STATE", "State", "Province",
            "Mailing_State", "Mailing State", "mailing-state",
        ])

        if not state_col:
            print_warn(f"  No MailingState column found — skipping.")
            continue

        df            = _flag_canadian(df, state_col)
        canadian_rows = df[df["_IsCanada"]].copy()
        canadian_rows = canadian_rows.drop(columns=["_IsCanada", "_MailingStateClean"])
        canadian_rows["SourceFile"] = f.name

        if canadian_rows.empty:
            print_done(f"  No Canadian rows found.")
        else:
            all_canadian.append(canadian_rows)
            print_done(f"  {len(canadian_rows):,} Canadian rows found.")

    if not all_canadian:
        print_warn("No Canadian rows found across all files. No output created.")
        return

    merged   = pd.concat(all_canadian, ignore_index=True)
    out_path = OUT_STEP2 / "canadian_locations_merged.xlsx"
    save_excel(merged, out_path)
    print_done(f"Merged {len(merged):,} Canadian rows → {out_path.name}")