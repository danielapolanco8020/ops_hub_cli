import re
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path

from config import OUT_STEP2, OUT_STEP1

from utils.file_helpers import (
    get_excel_files, get_files_by_cadence, read_excel, save_excel,
    prompt_int, prompt_yes_no, make_output_path,
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
    print("    %B %Y  → May 2025 (with space)")
    print("    %Y-%m  → 2025-05, 2024-01   (year-month number)")
    print("    %Y-%B  → 2025-May, 2024-January")
    print("    %Y-%b  → 2025-May, 2024-Jan")
    while True:
        fmt = input("  Date format [default: %B%Y]: ").strip()
        if fmt == "":
            return "%B%Y"
        try:
            datetime.strptime(datetime.now().strftime(fmt), fmt)
            return fmt
        except ValueError:
            print(f"  Invalid format '{fmt}'. Try again.")


# ── Skiptrace age bucketing ────────────────────────────────────────────────────

def _get_latest_tag_date(cell_value, prefix: str, date_fmt: str):
    """Return the most recent skiptrace tag date found in a cell, or None."""
    if not isinstance(cell_value, str):
        return None
    latest = None
    raw_tags = cell_value.replace(";", ",").split(",")
    for tag in [t.strip() for t in raw_tags]:
        idx = tag.lower().find(prefix.lower())
        if idx == -1:
            continue
        try:
            date_part = tag[idx + len(prefix):].strip()
            tag_date  = datetime.strptime(date_part, date_fmt)
            if latest is None or tag_date > latest:
                latest = tag_date
        except ValueError:
            continue
    return latest


def _print_age_buckets(df: pd.DataFrame, prefix: str, date_fmt: str,
                       current_date: datetime, label: str = ""):
    """Print how many properties fall into each skiptrace age bucket."""
    dates   = df["TAGS"].apply(lambda v: _get_latest_tag_date(v, prefix, date_fmt))
    has_tag = dates.notna()
    no_tag  = int((~has_tag).sum())

    under_1 = between = over_2 = 0
    if has_tag.any():
        ages = dates[has_tag].apply(lambda d: (current_date - d).days)
        under_1  = int((ages < 365).sum())
        between  = int(((ages >= 365) & (ages < 730)).sum())
        over_2   = int((ages >= 730).sum())

    header = f"Skiptrace age breakdown{f' — {label}' if label else ''}:"
    print_step(header)
    print(f"    Under 1 year       : {under_1:,}")
    print(f"    1 to 2 years       : {between:,}")
    print(f"    Over 2 years       : {over_2:,}")
    print(f"    No skiptrace tag   : {no_tag:,}")

    return under_1, between, over_2, no_tag


# ── Status logic ───────────────────────────────────────────────────────────────

def _determine_status(cell_value, cutoff: datetime, prefix: str, date_fmt: str) -> str:
    if not isinstance(cell_value, str):
        return "Active"

    has_old = has_recent = False
    raw_tags = cell_value.replace(";", ",").split(",")

    for tag in [t.strip() for t in raw_tags]:
        idx = tag.lower().find(prefix.lower())
        if idx == -1:
            continue
        try:
            date_part = tag[idx + len(prefix):].strip()
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

    current_date = datetime.now()

    run_cutoff  = prompt_yes_no("\n  Run cutoff analysis (flag tags older than N months)?", default=True)
    cutoff_date = None
    months      = None
    if run_cutoff:
        months      = prompt_int("  Cutoff in months", default=6, min_val=1)
        cutoff_date = current_date - relativedelta(months=months)

    print(f"\n  Tag prefix : '{prefix}'")
    print(f"  Date format: '{date_fmt}'")
    print(f"  Today      : {current_date.strftime('%Y-%m-%d')}")
    if run_cutoff:
        print(f"  Cutoff     : {cutoff_date.strftime('%Y-%m-%d')}  ({months} months ago)")
    else:
        print(f"  Cutoff     : skipped")
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

    run_age_breakdown = prompt_int(
        "\n  Run skiptrace age breakdown? (1 = Yes, 2 = No)",
        default=1, min_val=1, max_val=2
    ) == 1

    totals = [0, 0, 0, 0]  # under_1, between, over_2, no_tag

    for f in all_files:
        print_step(f"Processing: {f.name}")
        df = read_excel(f)
        if df is None:
            continue

        if "TAGS" not in df.columns:
            print_warn("  No TAGS column found — skipping.")
            continue

        if run_cutoff:
            df["Tag_Analysis"] = df["TAGS"].apply(
                lambda v: _determine_status(v, cutoff_date, prefix, date_fmt)
            )
            count_old = (df["Tag_Analysis"] == "OLDER_THAN_CUTOFF").sum()
            print_done(f"  {count_old:,} properties flagged as OLDER_THAN_CUTOFF")

        if run_age_breakdown:
            buckets = _print_age_buckets(df, prefix, date_fmt, current_date, label=f.name)
            for i, val in enumerate(buckets):
                totals[i] += val

        df = _fix_link_properties(df)

        out_path = OUT_STEP2 / f"tagged_{f.name}"
        save_excel(df, out_path)
        print_done(f"  Saved → {out_path.name}")

    if run_age_breakdown and len(all_files) > 1:
        print_step("Skiptrace age breakdown — TOTAL across all files:")
        print(f"    Under 1 year       : {totals[0]:,}")
        print(f"    1 to 2 years       : {totals[1]:,}")
        print(f"    Over 2 years       : {totals[2]:,}")
        print(f"    No skiptrace tag   : {totals[3]:,}")