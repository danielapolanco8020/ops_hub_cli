import re
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path

from config import OUT_STEP2, OUT_STEP1
from utils.file_helpers import (
    get_excel_files, get_files_by_cadence, read_excel, save_excel,
    prompt_int, prompt_cadence_or_all, make_output_path, resolve_input_dir,
    print_header, print_step, print_done, print_warn, print_error,
)


# ── Tag format prompt ──────────────────────────────────────────────────────────

def _prompt_tag_prefix() -> str:
    """Ask user for the skiptrace tag prefix used in their data."""
    print("\n  Enter the skiptrace tag prefix used in your TAGS column.")
    print("  Example: if tags look like 'SkiptraceMay2025', enter 'Skiptrace'")
    print("  Example: if tags look like 'Skip Trace Jan2025', enter 'Skip Trace'")
    while True:
        prefix = input("  Tag prefix: ").strip()
        if prefix:
            return prefix
        print("  Prefix cannot be empty.")


def _prompt_date_format(prefix: str) -> str:
    """Ask user for the date format used after the prefix."""
    print(f"\n  Enter the date format used after '{prefix}'.")
    print("  Examples:")
    print("    %B%Y   → May2025, January2024")
    print("    %b%Y   → May2025, Jan2024")
    print("    %m%Y   → 052025, 012024")
    print("    %B %Y  → May 2025 (with space)")
    while True:
        fmt = input("  Date format [default: %B%Y]: ").strip()
        if fmt == "":
            return "%B%Y"
        # Quick validation
        try:
            datetime.strptime(datetime.now().strftime(fmt), fmt)
            return fmt
        except ValueError:
            print(f"  Invalid format '{fmt}'. Try again.")


# ── Status logic ───────────────────────────────────────────────────────────────

def _determine_status(cell_value, cutoff: datetime, prefix: str, date_fmt: str) -> str:
    if not isinstance(cell_value, str):
        return "Active"

    has_old = has_recent = False

    for tag in [t.strip() for t in cell_value.split(",")]:
        if not tag.startswith(prefix):
            continue
        try:
            date_part = tag[len(prefix):].strip()
            tag_date  = datetime.strptime(date_part, date_fmt)
            if tag_date >= cutoff:
                has_recent = True
                break
            else:
                has_old = True
        except ValueError:
            continue

    if has_recent:  return "Active"
    if has_old:     return "OLDER_THAN_CUTOFF"
    return "Active"


# ── LINK PROPERTIES plain text fix ────────────────────────────────────────────

def _fix_link_properties(df: pd.DataFrame) -> pd.DataFrame:
    if "LINK PROPERTIES" not in df.columns:
        return df
    df["LINK PROPERTIES"] = df["LINK PROPERTIES"].astype(str).str.strip()
    # Strip Excel hyperlink formula if present: =HYPERLINK("url","label") → label
    def _extract(val):
        match = re.match(r'=HYPERLINK\("[^"]*",\s*"([^"]*)"\)', val, re.IGNORECASE)
        return match.group(1) if match else val
    df["LINK PROPERTIES"] = df["LINK PROPERTIES"].apply(_extract)
    return df


# ── Entry Point ────────────────────────────────────────────────────────────────

def run():
    print_header("STEP 2C — SKIPTRACE CHECK")

    # Prompt for cadence
    cadences = prompt_cadence_or_all("cadence to check")

    # Prompt once for tag structure
    prefix   = _prompt_tag_prefix()
    date_fmt = _prompt_date_format(prefix)

    months = prompt_int(
        "  Cutoff in months (tags older than this are flagged)",
        default=6, min_val=1
    )

    current_date = datetime.now()
    cutoff_date  = current_date - relativedelta(months=months)

    print(f"\n  Tag prefix : '{prefix}'")
    print(f"  Date format: '{date_fmt}'")
    print(f"  Today      : {current_date.strftime('%Y-%m-%d')}")
    print(f"  Cutoff     : {cutoff_date.strftime('%Y-%m-%d')}  ({months} months ago)")

    input_dir = resolve_input_dir([OUT_STEP2, OUT_STEP1])
    if not input_dir:
        print_error("No processed files found in any output folder.")
        return

    all_files = []
    for cadence in cadences:
        cadence_files = get_files_by_cadence(input_dir, cadence)
        if not cadence_files:
            print_warn(f"  No files found for cadence '{cadence}' in {input_dir.name}/")
        else:
            all_files.extend(cadence_files)

    if not all_files:
        print_error("No matching files found for selected cadence(s).")
        return

    print_step(f"Found {len(all_files)} file(s) in {input_dir.name}/")

    for f in all_files:
        print_step(f"Processing: {f.name}")
        df = read_excel(f)
        if df is None:
            continue

        if "TAGS" not in df.columns:
            print_warn("  No TAGS column found — skipping.")
            continue

        # Apply skiptrace tag analysis
        df["Tag_Analysis"] = df["TAGS"].apply(
            lambda v: _determine_status(v, cutoff_date, prefix, date_fmt)
        )
        count_old = (df["Tag_Analysis"] == "OLDER_THAN_CUTOFF").sum()

        # Fix LINK PROPERTIES to plain text
        df = _fix_link_properties(df)

        save_excel(df, f)
        print_done(f"  {count_old:,} properties flagged as OLDER_THAN_CUTOFF")
        print_done(f"  Saved → {f.name}")