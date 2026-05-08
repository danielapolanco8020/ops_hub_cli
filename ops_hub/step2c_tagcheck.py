import re
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path

from config import OUT_STEP2, OUT_STEP1

from utils.file_helpers import (
    get_excel_files, get_files_by_cadence, read_excel, save_excel,
    prompt_int, make_output_path,
    print_header, print_step, print_done, print_warn, print_error,
)


# ── Cadence multi-select ───────────────────────────────────────────────────────

def _prompt_cadences() -> list[str]:
    """Allow user to select one or multiple cadences."""
    options = {
        "1": "Direct Mail",
        "2": "Cold Calling",
        "3": "SMS",
    }
    print("\n  Select cadence(s) to check (enter numbers separated by spaces):", flush=True)
    print("    1. Direct Mail", flush=True)
    print("    2. Cold Calling", flush=True)
    print("    3. SMS", flush=True)
    print("    4. All", flush=True)
    while True:
        raw = input("  Your selection: ").strip()
        if raw == "4":
            return list(options.values())
        keys = raw.split()
        invalid = [k for k in keys if k not in options]
        if invalid:
            print(f"  Invalid option(s): {', '.join(invalid)}. Enter numbers from 1-4.")
            continue
        selected = [options[k] for k in keys]
        if selected:
            return selected
        print("  Select at least one option.")


# ── Tag format prompt ──────────────────────────────────────────────────────────

def _prompt_tag_prefix() -> str:
    print("\n  Enter the skiptrace tag prefix used in your TAGS column.")
    print("  Example: if tags look like 'SkiptraceMay2025', enter 'Skiptrace'")
    print("  Example: if tags look like 'Skip Trace Jan2025', enter 'Skip Trace'")
    while True:
        prefix = input("  Tag prefix: ").strip()
        if prefix:
            return prefix
        print("  Prefix cannot be empty.")


def _prompt_date_format(prefix: str) -> str:
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
        if not tag.lower().startswith(prefix.lower()):
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
    def _extract(val):
        match = re.match(r'=HYPERLINK\("[^"]*",\s*"([^"]*)"\)', val, re.IGNORECASE)
        return match.group(1) if match else val
    df["LINK PROPERTIES"] = df["LINK PROPERTIES"].apply(_extract)
    return df


# ── Entry Point ────────────────────────────────────────────────────────────────

def run():
    print_header("STEP 2C — SKIPTRACE CHECK")

    # Multi-select cadence prompt
    cadences = _prompt_cadences()

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
    print(f"  Cadences   : {', '.join(cadences)}")

    # Collect files per cadence using per-cadence folder resolution
    all_files = []
    for cadence in cadences:
        input_dir = None
        for folder in [OUT_STEP2, OUT_STEP1]:
            if get_files_by_cadence(folder, cadence):
                input_dir = folder
                break
        if not input_dir:
            print_warn(f"  No files found for cadence '{cadence}'")
            continue
        found = get_files_by_cadence(input_dir, cadence)
        print_step(f"Found {len(found)} '{cadence}' file(s) in {input_dir.name}/")
        all_files.extend(found)

    if not all_files:
        print_error("No matching files found for selected cadence(s).")
        return

    for f in all_files:
        print_step(f"Processing: {f.name}")
        df = read_excel(f)
        if df is None:
            continue

        if "TAGS" not in df.columns:
            print_warn("  No TAGS column found — skipping.")
            continue

        df["Tag_Analysis"] = df["TAGS"].apply(
            lambda v: _determine_status(v, cutoff_date, prefix, date_fmt)
        )
        count_old = (df["Tag_Analysis"] == "OLDER_THAN_CUTOFF").sum()

        df = _fix_link_properties(df)

        # Always save to step2_optional with tagged_ prefix
        out_path = OUT_STEP2 / f"tagged_{f.name}"
        save_excel(df, out_path)
        print_done(f"  {count_old:,} properties flagged as OLDER_THAN_CUTOFF")
        print_done(f"  Saved → {out_path.name}")